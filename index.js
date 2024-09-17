import pkg from "pg"
import { MeiliSearch } from "meilisearch"
import dotenv from "dotenv"
const { Client } = pkg
import { fetchBucketQuestions } from "./queries.js"
import { BATCH_SIZE, insertIntoMeiliSearch } from "./common.js"

dotenv.config()

const pgClient = new Client({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
})

const meiliClient = new MeiliSearch({
  host: process.env.MEILISEARCH_HOST,
  apiKey: process.env.MEILISEARCH_API_KEY,
})

async function syncQuestionsToMeiliSearch() {
  let offset = 0
  let questions

  try {
    await pgClient.connect()
    console.log("Connected to PostgreSQL.")

    do {
      questions = await fetchBucketQuestions(offset, pgClient)

      if (questions.length > 0) {
        await insertIntoMeiliSearch(questions, offset, meiliClient)
      }
      offset += BATCH_SIZE
    } while (questions.length === BATCH_SIZE)

    console.log("All bucket  questions have been synced to Meilisearch.")
    offset = 0
  } catch (err) {
    console.error("An error occurred during the sync process:", err)
  } finally {
    await pgClient.end()
    console.log("PostgreSQL connection closed.")
  }
}

syncQuestionsToMeiliSearch()
