import { BATCH_SIZE } from "./common.js"

//13361
export async function fetchBucketQuestions(offset = 0, pgClient) {
  try {
    const query = `select q.id ,q.question,q.client_id,q.question_text from questions q WHERE q.deleted_at IS null and q.is_custom_question = 'false'
                    LIMIT $1 OFFSET $2`
    const res = await pgClient.query(query, [BATCH_SIZE, offset])
    return res.rows
  } catch (err) {
    console.error("Error fetching questions from PostgreSQL:", err)
    throw err
  }
}
