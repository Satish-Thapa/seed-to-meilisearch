import { BATCH_SIZE } from "./common.js"
export async function fetchBucketQuestions(offset = 0, pgClient) {
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
