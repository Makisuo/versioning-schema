import { Effect, HashMap, Option, Schema, pipe } from "effect"
import {
	type MigrateError,
	type MigrationEntry,
	type MigrationFn,
	MigrationNotFoundError,
	SchemaNotFoundError,
	type SchemaVersion,
	TransformationError,
} from "./schema-migrators-errors" // Assuming errors are in './schema-migrators-errors'

/**
 * @interface SchemaMigrator
 * @description Interface for a service that handles data migration between schema versions.
 */
export interface SchemaMigrator {
	/**
	 * @description Migrates data from a specified version to another version by applying
	 * registered migration functions sequentially.
	 * @param data The raw input data (expected to conform to the 'fromVersion' schema).
	 * @param fromVersion The version number of the input data's schema.
	 * @param toVersion The target version number for the schema.
	 * @returns An Effect that resolves with the migrated data (typed as `any`),
	 *          or fails with a `MigrateError`. Runtime validation ensures the output
	 *          conforms to the `toVersion` schema.
	 * @template ToVersion The literal type of the target version number.
	 */
	readonly migrate: <ToVersion extends number>(
		data: unknown,
		fromVersion: number,
		toVersion: ToVersion,
	) => Effect.Effect<any, MigrateError>

	/**
	 * @description Gets the highest registered schema version number.
	 * @returns An `Option` containing the latest version number, or `None` if no versions are registered.
	 */
	readonly getLatestVersion: () => Option.Option<number>

	/**
	 * @description Retrieves the schema definition for a specific version.
	 * @param version The version number of the schema to retrieve.
	 * @returns An `Option` containing the `Schema.Schema<any>` object, or `None` if the version is not found.
	 */
	readonly getSchema: (version: number) => Option.Option<Schema.Schema<any>>

	/**
	 * @description Internal map of registered schemas.
	 * @internal
	 */
	readonly _schemas: HashMap.HashMap<number, SchemaVersion<any>>
	/**
	 * @description Internal map of registered migration functions.
	 * @internal
	 */
	readonly _migrations: HashMap.HashMap<string, MigrationEntry<any, any>>
}

/**
 * @class SchemaMigratorBuilder
 * @description Builder class for creating a SchemaMigrator instance.
 * Provides methods to define schema versions and the migrations between them.
 */
class SchemaMigratorBuilder {
	private schemas = HashMap.empty<number, SchemaVersion<any>>()
	private migrations = HashMap.empty<string, MigrationEntry<any, any>>()

	/**
	 * @private
	 * @method _addVersion
	 * @description Adds a schema definition for a specific version to the internal map.
	 * Issues a warning if overwriting an existing version with a different schema instance.
	 * @template A The type encoded/decoded by the schema.
	 * @param {number} version - The version number.
	 * @param {Schema.Schema<A>} schema - The schema definition for this version.
	 * @returns {this} The builder instance for chaining.
	 */
	private _addVersion<A>(version: number, schema: Schema.Schema<A>): this {
		const existing = HashMap.get(this.schemas, version)
		if (Option.isSome(existing) && existing.value.schema !== schema) {
			console.warn(`Schema version ${version} is being overwritten with a different schema instance.`)
		} else if (Option.isNone(existing)) {
			this.schemas = HashMap.set(this.schemas, version, {
				version,
				schema: schema as Schema.Schema<any>,
			})
		}
		return this
	}

	/**
	 * @private
	 * @method wrapMigration
	 * @description Wraps a user-provided migration function (sync or Effect) into a
	 * standardized Effect that handles errors and maps them to TransformationError.
	 * @template From The source data type for the migration.
	 * @template To The target data type for the migration.
	 * @param {number} fromVersion - The source version number.
	 * @param {number} toVersion - The target version number.
	 * @param {MigrationFn<From, To>} migrateFn - The user-provided migration function.
	 * @returns {(data: From) => Effect.Effect<To, TransformationError>} An Effect-wrapped migration function.
	 */
	private wrapMigration<From, To>(
		fromVersion: number,
		toVersion: number,
		migrateFn: MigrationFn<From, To>,
	): (data: From) => Effect.Effect<To, TransformationError> {
		return (data: From) =>
			Effect.sync(() => migrateFn(data)).pipe(
				Effect.flatMap((result) =>
					Effect.isEffect(result)
						? Effect.mapError(result, (cause) => new TransformationError({ fromVersion, toVersion, cause }))
						: Effect.succeed(result),
				),
				Effect.catchAll((cause) => {
					if (cause instanceof TransformationError) {
						return Effect.fail(cause)
					}
					return Effect.fail(new TransformationError({ fromVersion, toVersion, cause }))
				}),
			)
	}

	/**
	 * @method addMigration
	 * @description Adds a migration definition between two adjacent versions (V and V+1).
	 * Requires defining both the upgrade (V -> V+1) and downgrade (V+1 -> V) functions.
	 * Automatically registers the schemas for both versions if not already present.
	 * @template From The type of the lower version schema (V).
	 * @template To The type of the higher version schema (V+1).
	 * @param {number} version - The lower of the two adjacent versions (V).
	 * @param {Schema.Schema<From>} fromSchema - The schema for the lower version (V).
	 * @param {Schema.Schema<To>} toSchema - The schema for the higher version (V+1).
	 * @param {MigrationFn<From, To>} upgradeFn - Function to transform data from `fromSchema` type to `toSchema` type.
	 * @param {MigrationFn<To, From>} downgradeFn - Function to transform data from `toSchema` type to `fromSchema` type.
	 * @returns {this} The builder instance for chaining.
	 */
	addMigration<From, To>(
		version: number,
		fromSchema: Schema.Schema<From>,
		toSchema: Schema.Schema<To>,
		upgradeFn: MigrationFn<From, To>,
		downgradeFn: MigrationFn<To, From>,
	): this {
		const fromVersion = version
		const toVersion = version + 1

		this._addVersion(fromVersion, fromSchema)
		this._addVersion(toVersion, toSchema)

		const upgradeKey = `${fromVersion}-${toVersion}`
		if (HashMap.has(this.migrations, upgradeKey)) {
			console.warn(`Upgrade migration ${upgradeKey} is being overwritten.`)
		}
		const wrappedUpgrade = this.wrapMigration(fromVersion, toVersion, upgradeFn)
		this.migrations = HashMap.set(this.migrations, upgradeKey, {
			fromVersion,
			toVersion,
			migrate: wrappedUpgrade,
		})

		const downgradeKey = `${toVersion}-${fromVersion}`
		if (HashMap.has(this.migrations, downgradeKey)) {
			console.warn(`Downgrade migration ${downgradeKey} is being overwritten.`)
		}
		const wrappedDowngrade = this.wrapMigration(toVersion, fromVersion, downgradeFn)
		this.migrations = HashMap.set(this.migrations, downgradeKey, {
			fromVersion: toVersion,
			toVersion: fromVersion,
			migrate: wrappedDowngrade,
		})

		return this
	}

	/**
	 * @method build
	 * @description Finalizes the builder configuration and returns a `SchemaMigrator` instance.
	 * @returns {SchemaMigrator} The configured schema migrator service.
	 */
	build(): SchemaMigrator {
		const finalSchemas = this.schemas
		const finalMigrations = this.migrations

		let maxVersion: number | undefined = undefined
		for (const version of HashMap.keys(finalSchemas)) {
			if (maxVersion === undefined || version > maxVersion) {
				maxVersion = version
			}
		}
		const latestVersion: Option.Option<number> = maxVersion === undefined ? Option.none() : Option.some(maxVersion)

		const getSchema = (version: number): Option.Option<Schema.Schema<any>> =>
			pipe(
				HashMap.get(finalSchemas, version),
				Option.map((sv) => sv.schema),
			)

		const migrate = <ToVersion extends number>(
			data: unknown,
			fromVersion: number,
			toVersion: ToVersion,
		): Effect.Effect<any, MigrateError> => {
			const targetSchemaOpt = HashMap.get(finalSchemas, toVersion)
			if (Option.isNone(targetSchemaOpt)) {
				return Effect.fail(new SchemaNotFoundError({ version: toVersion }))
			}
			const targetSchema = targetSchemaOpt.value.schema

			if (fromVersion === toVersion) {
				const currentSchemaOpt = HashMap.get(finalSchemas, fromVersion)
				if (Option.isNone(currentSchemaOpt)) {
					return Effect.fail(new SchemaNotFoundError({ version: fromVersion }))
				}
				return Schema.decodeUnknown(currentSchemaOpt.value.schema)(data)
			}

			return Effect.gen(function* (_) {
				const startSchemaOpt = HashMap.get(finalSchemas, fromVersion)
				if (Option.isNone(startSchemaOpt)) {
					return yield* _(Effect.fail(new SchemaNotFoundError({ version: fromVersion })))
				}
				let currentData = yield* _(Schema.decodeUnknown(startSchemaOpt.value.schema)(data))

				const direction = fromVersion < toVersion ? 1 : -1
				const steps: Array<[number, number]> = []
				for (let v = fromVersion; v !== toVersion; v += direction) {
					steps.push([v, v + direction])
				}

				for (const [currentV, nextV] of steps) {
					const migrationKey = `${currentV}-${nextV}`
					const migrationOpt = HashMap.get(finalMigrations, migrationKey)
					if (Option.isNone(migrationOpt)) {
						return yield* _(
							Effect.fail(
								new MigrationNotFoundError({
									fromVersion: currentV,
									toVersion: nextV,
								}),
							),
						)
					}
					const migration = migrationOpt.value
					currentData = yield* _(migration.migrate(currentData))
				}

				const validatedData = yield* _(Schema.encode(targetSchema)(currentData))
				return validatedData
			})
		}

		return {
			migrate,
			getLatestVersion: () => latestVersion,
			getSchema,
			_schemas: finalSchemas,
			_migrations: finalMigrations,
		}
	}
}

/**
 * @function createMigrator
 * @description Factory function to create a new `SchemaMigratorBuilder` instance.
 * @returns {SchemaMigratorBuilder} A new builder instance.
 */
export const createMigrator = () => new SchemaMigratorBuilder()
