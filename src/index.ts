import { Console, Effect, Schema, pipe } from "effect"
import { createMigrator } from "./schema-migrator"

const UserV1 = Schema.Struct({
	id: Schema.Number,
	name: Schema.String,
})
type UserV1 = Schema.Schema.Type<typeof UserV1>

const UserV2 = Schema.Struct({
	id: Schema.Number,
	name: Schema.String,
	email: Schema.NullOr(Schema.String),
})
type UserV2 = Schema.Schema.Type<typeof UserV2>

const UserV3 = Schema.Struct({
	id: Schema.Number,
	firstName: Schema.String,
	lastName: Schema.String,
	email: Schema.String,
})

type UserV3 = Schema.Schema.Type<typeof UserV3>

const userMigrator = createMigrator()
	.addVersion(1, UserV1)
	.addUpgrade<UserV1, UserV2>(1, UserV2, (userV1) => ({
		...userV1,
		email: null,
	}))
	.addDowngrade<UserV2, UserV1>(2, UserV1, (userV2) => ({
		id: userV2.id,
		name: userV2.name,
	}))
	.addUpgrade<UserV2, UserV3>(2, UserV3, (userV2) => {
		if (!userV2.email) {
			const names = userV2.name.split(" ")
			return {
				id: userV2.id,
				firstName: names[0] ?? "Unknown",
				lastName: names.slice(1).join(" ") || "User",
				email: "default@example.com",
			}
		}

		const names = userV2.name.split(" ")
		return {
			id: userV2.id,
			firstName: names[0] ?? "Unknown",
			lastName: names.slice(1).join(" ") || "User",
			email: userV2.email,
		}
	})
	.addDowngrade<UserV3, UserV2>(3, UserV2, (userV3) => ({
		id: userV3.id,
		name: `${userV3.firstName} ${userV3.lastName}`,
		email: userV3.email,
	}))
	.build()

// --- Test Migrations ---

const userDataV1 = { id: 1, name: "Alice" }
const userDataV2WithEmail = {
	id: 2,
	name: "Bob Smith",
	email: "bob@example.com",
}
const userDataV2NoEmail = { id: 3, name: "Charlie Day", email: null }

const program = Effect.gen(function* () {
	yield* Console.log("--- Testing Migrations ---")

	// Migrate V1 -> V3
	const userV3FromV1 = yield* pipe(
		userMigrator.migrate(userDataV1, 1, 3),
		Effect.tap((u) => Console.log("V1 -> V3:", u)),
	)

	// Migrate V3 -> V1 (data loss)
	const userV1FromV3 = yield* pipe(
		userMigrator.migrate(userV3FromV1, 3, 1),
		Effect.tap((u) => Console.log("V3 -> V1:", u)),
	)

	// Migrate V2 (with email) -> V3
	const userV3FromV2 = yield* pipe(
		userMigrator.migrate(userDataV2WithEmail, 2, 3),
		Effect.tap((u) => Console.log("V2 (email) -> V3:", u)),
	)

	// Migrate V2 (no email) -> V3 (using default)
	const userV3FromV2NoEmail = yield* pipe(
		userMigrator.migrate(userDataV2NoEmail, 2, 3),
		Effect.tap((u) => Console.log("V2 (no email) -> V3:", u)),
	)

	// Migrate V1 -> V1 (just parsing)
	const userV1FromV1 = yield* pipe(
		userMigrator.migrate(userDataV1, 1, 1),
		Effect.tap((u) => Console.log("V1 -> V1:", u)),
	)

	// // Schema not found
	// const schemaNotFound = yield* pipe(
	// 	userMigrator.migrate(userDataV1, 0, 1), // Version 0 doesn't exist
	// 	Effect.flip, // Get the error
	// 	Effect.tap((e) => Console.log("\nSchema Not Found Error:", e)),
	// );

	// // Migration not found
	// const migrationNotFound = yield* pipe(
	// 	userMigrator.migrate(userDataV1, 1, 4),
	// 	Effect.flip,
	// 	Effect.tap((e) => Console.log("\nMigration Not Found Error:", e)),
	// );

	// // Invalid initial data
	// const parseError = yield* pipe(
	// 	userMigrator.migrate({ id: "abc", name: 123 }, 1, 2), /
	// 	Effect.flip,
	// 	Effect.tap((e) => Console.log("\nParse Error:", e)),
	// );
})

Effect.runPromise(program)
