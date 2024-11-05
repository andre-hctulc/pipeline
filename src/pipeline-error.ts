import type { PipelineCheckpoint } from "./pipeline-checkpoint";

/**
 * @param uncaught Whether the error was uncaught
 */
export class PipelineError extends Error {
    constructor(
        message: string,
        readonly source: PipelineCheckpoint<any, any>,
        override readonly cause: unknown,
        readonly uncaught = false
    ) {
        super(`Error in pipeline (source '${source.id}'): ${message}`);
        this.name = "PipelineError";
    }
}

export class PipelineRollbackError extends PipelineError {
    constructor(
        source: PipelineCheckpoint<any, any>,
        readonly rollbacksFailed: [checkpoint: PipelineCheckpoint<any, any>, error: unknown][]
    ) {
        super("Rollback failed on flush error", source, rollbacksFailed, false);
        this.name = "PipelineRollbackError";
    }
}
