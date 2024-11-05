import type { MaybePromise } from "@andre-hctulc/util";
import { PipelineError, PipelineRollbackError } from "./pipeline-error";

export type PipelineOut<T extends PipelineCheckpoint<any, any>> = T extends PipelineCheckpoint<any, infer O>
    ? O
    : never;

export type PipelineIn<T extends PipelineCheckpoint<any, any>> = T extends PipelineCheckpoint<infer I, any>
    ? I
    : never;

/**
 * A node in the pipeline graph.
 * @template I - Input data type
 * @template O - Output data type
 * @template C - Context
 */
export abstract class PipelineCheckpoint<I, O, C extends object = {}> {
    private _pre: PipelineCheckpoint<any, I>[];
    private _next: PipelineCheckpoint<O, any>[];
    protected ctx: C = {} as C;

    constructor(
        readonly id: string,
        pre: PipelineCheckpoint<any, I>[] = [],
        next: PipelineCheckpoint<O, any>[] = [],
        context?: C
    ) {
        this._pre = pre;
        this._next = next;
        if (context) this.ctx = context;
    }

    // --- Interface ---

    abstract pipe(input: I): MaybePromise<O>;

    /**
     * Rollback the checkpoint.
     * @param input The input data
     * @param index The index of the checkpoint in the execution chain
     */
    rollback?(input: I, index: number): MaybePromise<void>;

    /**
     * Override this method to handle errors in the pipeline.
     *
     * When this returns a value, the pipeline continues with the returned value.
     * Otherwise the default error handling is applied.
     */
    pipeOnError?(input: I, error: unknown): MaybePromise<O> | undefined;

    /**
     * Override this method to implement custom decision logic.
     *
     * By default the first next checkpoint is used.
     */
    protected decide(
        input: I,
        output: O,
        choices: PipelineCheckpoint<O, any>[]
    ): PipelineCheckpoint<O, any> | null {
        return choices[0] || null;
    }

    // --- Api ---

    /**
     * When an error occurs in the pipeline, the error is caught by the nearest `PipelineErrorBoundary` checkpoint. If there is no such checkpoint, the error is thrown as a `PipelineError`.
     *
     * Rollbacks are performed, starting from the error source checkpoint to the start of the execution chain. If rollbacks fail, a `PipelineRollbackError` is thrown.
     *
     * @param input
     * @param disableRollbacks
     * @returns The output of the last checkpoint in the pipeline or null, if the pipeline has a `PipelineErrorBoundary` prior to the error source checkpoint
     */
    async flush<T extends PipelineCheckpoint<any, any> = PipelineCheckpoint<any, any>>(
        input: I,
        disableRollbacks = false
    ): Promise<PipelineOut<T> | null> {
        const rollback = async (
            source: PipelineCheckpoint<any, any>,
            chain: [checkpoint: PipelineCheckpoint<any, any>, input: any][]
        ) => {
            let i = 0;
            const errors: [PipelineCheckpoint<any, any>, unknown][] = [];

            for (const [checkpoint, input] of chain.reverse()) {
                ++i;
                if (!checkpoint.rollback) continue;

                try {
                    await checkpoint.rollback(input, chain.length - i);
                } catch (error) {
                    errors.push([checkpoint, error]);
                }
            }

            if (errors.length) throw new PipelineRollbackError(source, errors);
        };

        const decideNext = (
            checkpoint: PipelineCheckpoint<any, any>,
            input: any,
            out: any,
            choices: PipelineCheckpoint<any, any>[]
        ) => {
            if (!checkpoint) return null;
            const next = checkpoint?.decide(input, out, choices);
            if (!next) return null;
            return next;
        };

        let out: any = input;
        let errorBoundary: PipelineErrorBoundary<any> | null = null;
        let checkpoint: PipelineCheckpoint<any, any> | null = this;
        /** execution chain */
        const chain: [checkpoint: PipelineCheckpoint<any, any>, input: any][] = [];

        while (checkpoint) {
            let input = out;

            // capture chain and error boundary
            chain.push([checkpoint, input]);
            if (checkpoint instanceof PipelineErrorBoundary) errorBoundary = checkpoint;

            try {
                // pipe
                out = await checkpoint.pipe(input);
                checkpoint = decideNext(checkpoint, input, out, checkpoint._next);
            } catch (error) {
                // If pipeOnError is defined, use it to handle the error
                if (this.pipeOnError) {
                    const o = await this.pipeOnError(input, error);

                    if (o !== undefined) {
                        out = o;
                        checkpoint = decideNext(checkpoint!, input, out, checkpoint?._next || []);
                        continue;
                    }
                }

                // rollback
                if (!disableRollbacks) {
                    await rollback(checkpoint!, chain);
                }

                // Error boundary handling
                if (errorBoundary) {
                    errorBoundary.handleError(this, error);
                    return null;
                }
                // Uncaught
                else {
                    if (error instanceof PipelineError) throw error;
                    throw new PipelineError("Failed to flush", this, error, true);
                }
            }
        }

        return out as any;
    }

    /**
     * Connects the current checkpoint to the next one
     * @param next The next checkpoint
     * @returns `next`
     */
    weld<T extends PipelineCheckpoint<O, any>>(next: T): T {
        // disconnect old nodes
        this.getNext().forEach((n) => n.removePre(this));
        // connect new next
        this.setNext([next]);
        next.setPre([this]);
        // return next
        return next;
    }

    /**
     * @returns The first checkpoint in the pipeline when following the first pre checkpoint
     */
    inlet<T extends PipelineCheckpoint<any, any>>(): T {
        let start: PipelineCheckpoint<any, any> = this;
        while (start.hasPre()) {
            start = start.getPre()[0];
        }
        return start as T;
    }

    /**
     * @returns The last checkpoint in the pipeline when following the first next checkpoint
     */
    outlet<T extends PipelineCheckpoint<any, any>>(): T {
        let end: PipelineCheckpoint<any, any> = this;
        while (end.hasNext()) {
            end = end.getNext()[0];
        }
        return end as T;
    }

    forEach(callback: (checkpoint: PipelineCheckpoint<any, any>) => void): this {
        const forEach = (
            checkpoint: PipelineCheckpoint<any, any>,
            visited: Set<PipelineCheckpoint<any, any>>
        ) => {
            if (visited.has(checkpoint)) return;
            visited.add(checkpoint);
            this._pre.forEach((pre) => forEach(pre, visited));
            callback(checkpoint);
            this._next.forEach((next) => forEach(next, visited));
        };

        forEach(this, new Set<PipelineCheckpoint<any, any>>());

        return this;
    }

    /**
     * Set the context fir all checkpoints in the pipeline or only for this checkpoint
     * @param context The context
     * @param distribute Set for all checkpoints in the pipeline. Default: true
     */
    setContext(context: C, distribute = true): this {
        if (distribute) this.forEach((cp) => (cp.ctx = context));
        else this.ctx = context;
        return this;
    }

    // --- Graph Operations ---

    /** Graph Operation */
    hasNext(): boolean {
        return this._next.length > 0;
    }

    /** Graph Operation */
    hasPre(): boolean {
        return this._pre.length > 0;
    }

    /** Graph Operation */
    isFirst(): boolean {
        return this._pre == null;
    }

    /** Graph Operation */
    getPre(): PipelineCheckpoint<any, I>[] {
        return this._pre;
    }

    /** Graph Operation */
    setPre(pre: PipelineCheckpoint<any, I>[]): void {
        this._pre = pre;
    }

    /** Graph Operation */
    addPre(pre: PipelineCheckpoint<any, I>): void {
        // remove first, so the checkpoint is not added twice and is at the end of the list
        this.removePre(pre);
        this._pre.push(pre);
    }

    /** Graph Operation */
    removePre(pre: PipelineCheckpoint<any, I>): void {
        this._pre = this._pre.filter((p) => p !== pre);
    }

    /** Graph Operation */
    getNext(): PipelineCheckpoint<O, any>[] {
        return this._next;
    }

    /** Graph Operation */
    setNext(next: PipelineCheckpoint<any, any>[]): void {
        this._next = next;
    }

    /** Graph Operation */
    removeNext(next: PipelineCheckpoint<any, any>): void {
        this._next = this._next.filter((n) => n !== next);
    }

    /** Graph Operation */
    addNext(next: PipelineCheckpoint<any, any>): void {
        // remove first, so the checkpoint is not added twice and is at the end of the list
        this.removeNext(next);
        this._next.push(next);
    }
}

export abstract class PipelineErrorBoundary<T> extends PipelineCheckpoint<T, T> {
    override async pipe(input: T): Promise<T> {
        return input;
    }

    protected abstract onError(error: PipelineError): void;

    handleError(source: PipelineCheckpoint<any, any>, error: unknown) {
        const err =
            error instanceof PipelineError ? error : new PipelineError("Failed to flush", source, error);
        this.onError(err);
    }
}
