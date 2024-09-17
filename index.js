import pkg from "pg"
import { MeiliSearch } from "meilisearch"
import dotenv from "dotenv"
const { Client } = pkg

dotenv.config()

const BATCH_SIZE = 1000
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

async function fetchQuestions(offset = 0) {
  try {
    const query = `select q.id ,q.parent_id ,q.primary_question_id ,q.question ,q.question_text, q.deleted_at, b.id as bucket_id,c.id as course_id from questions q 
                    inner join buckets b on q.bucket_id  = b.id inner join courses c on b.course_id = c.id 
                    WHERE q.deleted_at IS null 
                    LIMIT $1 OFFSET $2`
    const res = await pgClient.query(query, [BATCH_SIZE, offset])
    return res.rows
  } catch (err) {
    console.error("Error fetching questions from PostgreSQL:", err)
    throw err
  }
}

async function insertIntoMeiliSearch(questions, offset) {
  try {
    const index = meiliClient.index("testpaper-questions")
    const res = await index.addDocuments(questions, { primaryKey: "id" })

    await pollTaskStatus(res.taskUid, 1000)

    console.log(`Inserted ${questions.length} questions into Meilisearch.`)
  } catch (err) {
    const ids = questions.map((question) => question.id)
    console.error(
      "Error inserting questions into Meilisearch:",
      err,
      "ids:",
      ids,
      "offset",
      offset
    )
    throw err
  }
}

async function syncQuestionsToMeiliSearch() {
  let offset = 0
  let questions

  try {
    await pgClient.connect()
    console.log("Connected to PostgreSQL.")

    do {
      questions = await fetchQuestions(offset)

      if (questions.length > 0) {
        await insertIntoMeiliSearch(questions)
      }
      offset += BATCH_SIZE
    } while (questions.length === BATCH_SIZE)

    console.log("All questions have been synced to Meilisearch.")
  } catch (err) {
    console.error("An error occurred during the sync process:", err)
  } finally {
    await pgClient.end()
    console.log("PostgreSQL connection closed.")
  }
}

syncQuestionsToMeiliSearch()

async function pollTaskStatus(taskUid, interval = 1000) {
  const checkTask = async () => {
    const taskStatus = await meiliClient.getTask(taskUid)
    console.log(`Task Status [${taskUid}]: ${taskStatus.status}`)

    if (taskStatus.status === "succeeded") {
      console.log("Task succeeded!")
      clearInterval(pollInterval)
    } else if (taskStatus.status === "failed") {
      console.error("Task failed:", taskStatus.error)
      clearInterval(pollInterval)
      throw new Error("Failed to add questions")
    }
  }

  const pollInterval = setInterval(checkTask, interval)
}
