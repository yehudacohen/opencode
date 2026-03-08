import { eq } from "drizzle-orm"
import { Effect, Layer, Option, Schema, ServiceMap } from "effect"

import { Database } from "@/storage/db"
import { AccountStateTable, AccountTable } from "./account.sql"
import { Account, AccountID, AccountRepoError, OrgID } from "./schema"

export type AccountRow = (typeof AccountTable)["$inferSelect"]

const decodeAccount = Schema.decodeUnknownSync(Account)

type DbClient = Parameters<typeof Database.use>[0] extends (db: infer T) => unknown ? T : never

const toAccountRepoError = (message: string, cause?: unknown) => new AccountRepoError({ message, cause })

const db = <A>(run: (db: DbClient) => A) =>
  Effect.try({
    try: () => Database.use(run),
    catch: (cause) => toAccountRepoError("Database operation failed", cause),
  })

const fromRow = (row: AccountRow) => decodeAccount(row)

const current = (db: DbClient) => {
  const state = db.select().from(AccountStateTable).where(eq(AccountStateTable.id, 1)).get()
  if (!state?.active_account_id) return
  return db.select().from(AccountTable).where(eq(AccountTable.id, state.active_account_id)).get()
}

const setActive = (db: DbClient, accountID: AccountID) =>
  db
    .insert(AccountStateTable)
    .values({ id: 1, active_account_id: accountID })
    .onConflictDoUpdate({
      target: AccountStateTable.id,
      set: { active_account_id: accountID },
    })
    .run()

export class AccountRepo extends ServiceMap.Service<
  AccountRepo,
  {
    readonly active: () => Effect.Effect<Option.Option<Account>, AccountRepoError>
    readonly list: () => Effect.Effect<Account[], AccountRepoError>
    readonly remove: (accountID: AccountID) => Effect.Effect<void, AccountRepoError>
    readonly use: (accountID: AccountID, orgID: Option.Option<OrgID>) => Effect.Effect<void, AccountRepoError>
    readonly getRow: (accountID: AccountID) => Effect.Effect<Option.Option<AccountRow>, AccountRepoError>
    readonly persistToken: (input: {
      accountID: AccountID
      accessToken: string
      refreshToken: string
      expiry: Option.Option<number>
    }) => Effect.Effect<void, AccountRepoError>
    readonly persistAccount: (input: {
      id: AccountID
      email: string
      url: string
      accessToken: string
      refreshToken: string
      expiry: number
      orgID: Option.Option<OrgID>
    }) => Effect.Effect<void, AccountRepoError>
  }
>()("@opencode/AccountRepo") {
  static readonly layer: Layer.Layer<AccountRepo> = Layer.succeed(
    AccountRepo,
    AccountRepo.of({
      active: Effect.fn("AccountRepo.active")(() =>
        db((db) => current(db)).pipe(Effect.map((row) => (row ? Option.some(fromRow(row)) : Option.none()))),
      ),

      list: Effect.fn("AccountRepo.list")(() => db((db) => db.select().from(AccountTable).all().map(fromRow))),

      remove: Effect.fn("AccountRepo.remove")((accountID: AccountID) =>
        db((db) =>
          Database.transaction((tx) => {
            tx.update(AccountStateTable)
              .set({ active_account_id: null })
              .where(eq(AccountStateTable.active_account_id, accountID))
              .run()
            tx.delete(AccountTable).where(eq(AccountTable.id, accountID)).run()
          }),
        ).pipe(Effect.asVoid),
      ),

      use: Effect.fn("AccountRepo.use")((accountID: AccountID, orgID: Option.Option<OrgID>) =>
        db((db) =>
          Database.transaction((tx) => {
            tx.update(AccountTable)
              .set({ selected_org_id: Option.getOrNull(orgID) })
              .where(eq(AccountTable.id, accountID))
              .run()
            setActive(tx, accountID)
          }),
        ).pipe(Effect.asVoid),
      ),

      getRow: Effect.fn("AccountRepo.getRow")((accountID: AccountID) =>
        db((db) => db.select().from(AccountTable).where(eq(AccountTable.id, accountID)).get()).pipe(
          Effect.map(Option.fromNullishOr),
        ),
      ),

      persistToken: Effect.fn("AccountRepo.persistToken")((input) =>
        db((db) =>
          db
            .update(AccountTable)
            .set({
              access_token: input.accessToken,
              refresh_token: input.refreshToken,
              token_expiry: Option.getOrNull(input.expiry),
            })
            .where(eq(AccountTable.id, input.accountID))
            .run(),
        ).pipe(Effect.asVoid),
      ),

      persistAccount: Effect.fn("AccountRepo.persistAccount")((input) => {
        const orgID = Option.getOrNull(input.orgID)
        return Effect.try({
          try: () =>
            Database.transaction((tx) => {
              tx.insert(AccountTable)
                .values({
                  id: input.id,
                  email: input.email,
                  url: input.url,
                  access_token: input.accessToken,
                  refresh_token: input.refreshToken,
                  token_expiry: input.expiry,
                  selected_org_id: orgID,
                })
                .onConflictDoUpdate({
                  target: AccountTable.id,
                  set: {
                    access_token: input.accessToken,
                    refresh_token: input.refreshToken,
                    token_expiry: input.expiry,
                    selected_org_id: orgID,
                  },
                })
                .run()
              setActive(tx, input.id)
            }),
          catch: (cause) => toAccountRepoError("Database operation failed", cause),
        }).pipe(Effect.asVoid)
      }),
    }),
  )
}
