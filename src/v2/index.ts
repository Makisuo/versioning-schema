import { Console, Effect, Schema, pipe } from "effect"
import { createMigrator } from "./schema-migrator"

const UserV1 = Schema.Struct({ id: Schema.Number, name: Schema.String })
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
	.addMigration<UserV1, UserV2>(
		1,
		UserV1,
		UserV2,
		(userV1) => ({
			...userV1,
			email: null,
		}),
		(userV2) => ({
			id: userV2.id,
			name: userV2.name,
		}),
	)
	.addMigration<UserV2, UserV3>(
		2,
		UserV2,
		UserV3,
		(userV2) => {
			const names = userV2.name.split(" ")
			const firstName = names[0] ?? "Unknown"
			const lastName = names.slice(1).join(" ") || "User"

			// Handle missing email - provide default or fail
			if (!userV2.email) {
				return {
					id: userV2.id,
					firstName,
					lastName,
					email: "default@example.com",
				}
			}
			return {
				id: userV2.id,
				firstName,
				lastName,
				email: userV2.email,
			}
		},
		(userV3) => ({
			id: userV3.id,
			name: `${userV3.firstName} ${userV3.lastName}`,
			email: userV3.email,
		}),
	)
	.build()

// --- Test Migrations (same test logic as before) ---

const userDataV1 = { id: 1, name: "Alice" }
const userDataV2WithEmail = { id: 2, name: "Bob Smith", email: "bob@example.com" }
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
})

Effect.runPromise(program)
