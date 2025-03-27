import { Data, type Effect } from "effect"
import type { ParseError } from "effect/ParseResult"
import type * as Schema from "effect/Schema"

export class MigrationError extends Data.TaggedError("MigrationError")<{
	readonly message: string
	readonly cause?: unknown
}> {}

export class SchemaNotFoundError extends Data.TaggedError("SchemaNotFoundError")<{
	readonly version: number
}> {
	get message() {
		return `Schema for version ${this.version} not found.`
	}
}

export class MigrationNotFoundError extends Data.TaggedError("MigrationNotFoundError")<{
	readonly fromVersion: number
	readonly toVersion: number
}> {
	get message() {
		return `Migration from version ${this.fromVersion} to ${this.toVersion} not found.`
	}
}

export class TransformationError extends Data.TaggedError("TransformationError")<{
	readonly fromVersion: number
	readonly toVersion: number
	readonly cause: unknown
}> {
	get message() {
		return `Error during transformation from version ${this.fromVersion} to ${this.toVersion}.`
	}
}

export type MigrateError =
	| ParseError
	| SchemaNotFoundError
	| MigrationNotFoundError
	| TransformationError
	| MigrationError

export interface SchemaVersion<A = unknown> {
	readonly version: number
	readonly schema: Schema.Schema<A>
}

export type MigrationFn<From, To> = (data: From) => To | Effect.Effect<To, MigrationError>

export interface MigrationEntry<From = unknown, To = unknown> {
	readonly fromVersion: number
	readonly toVersion: number
	readonly migrate: (data: From) => Effect.Effect<To, TransformationError>
}
