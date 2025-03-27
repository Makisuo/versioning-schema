import { Effect, HashMap, Option, pipe } from "effect"
import * as Schema from "effect/Schema"
import {
	type MigrateError,
	type MigrationEntry,
	type MigrationFn,
	MigrationNotFoundError,
	SchemaNotFoundError,
	type SchemaVersion,
	TransformationError,
} from "./schema-migrators-errors"

// --- Service Interface ---

export interface SchemaMigrator {
	readonly migrate: <ToVersion extends number>(
		data: unknown,
		fromVersion: number,
		toVersion: ToVersion,
	) => Effect.Effect<unknown, MigrateError>
	readonly getLatestVersion: () => Option.Option<number>
	readonly getSchema: (version: number) => Option.Option<Schema.Schema<unknown>>
	readonly _schemas: HashMap.HashMap<number, SchemaVersion<unknown>>
	readonly _migrations: HashMap.HashMap<string, MigrationEntry<unknown, unknown>>
}

// --- Builder Implementation ---
class SchemaMigratorBuilder {
	private schemas = HashMap.empty<number, SchemaVersion<any>>()
	private migrations = HashMap.empty<string, MigrationEntry<any, any>>()

	addVersion<A>(version: number, schema: Schema.Schema<A>): this {
		if (HashMap.has(this.schemas, version)) {
			console.warn(`Schema version ${version} is being overwritten.`)
		}
		this.schemas = HashMap.set(this.schemas, version, {
			version,
			schema: schema as Schema.Schema<any>,
		})
		return this
	}

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

	addUpgrade<From, To>(fromVersion: number, toSchema: Schema.Schema<To>, migrateFn: MigrationFn<From, To>): this {
		const toVersion = fromVersion + 1
		if (!HashMap.has(this.schemas, fromVersion)) {
			throw new Error(
				`Cannot add upgrade from version ${fromVersion}: Source schema not found. Add version ${fromVersion} first.`,
			)
		}
		this.addVersion(toVersion, toSchema)
		const migrationKey = `${fromVersion}-${toVersion}`
		if (HashMap.has(this.migrations, migrationKey)) {
			console.warn(`Migration ${migrationKey} is being overwritten.`)
		}
		const wrappedMigrate = this.wrapMigration(fromVersion, toVersion, migrateFn)
		this.migrations = HashMap.set(this.migrations, migrationKey, {
			fromVersion,
			toVersion,
			migrate: wrappedMigrate,
		})
		return this
	}

	addDowngrade<From, To>(fromVersion: number, toSchema: Schema.Schema<To>, migrateFn: MigrationFn<From, To>): this {
		const toVersion = fromVersion - 1
		if (toVersion < 0) {
			throw new Error("Cannot add downgrade to a version less than 0.")
		}
		if (!HashMap.has(this.schemas, fromVersion)) {
			throw new Error(
				`Cannot add downgrade from version ${fromVersion}: Source schema not found. Add version ${fromVersion} first.`,
			)
		}
		this.addVersion(toVersion, toSchema)
		const migrationKey = `${fromVersion}-${toVersion}`
		if (HashMap.has(this.migrations, migrationKey)) {
			console.warn(`Migration ${migrationKey} is being overwritten.`)
		}
		const wrappedMigrate = this.wrapMigration(fromVersion, toVersion, migrateFn)
		this.migrations = HashMap.set(this.migrations, migrationKey, {
			fromVersion,
			toVersion,
			migrate: wrappedMigrate,
		})
		return this
	}

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

export const createMigrator = () => new SchemaMigratorBuilder()
