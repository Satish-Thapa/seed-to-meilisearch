import { BATCH_SIZE } from "./common.js"

//13361
export async function fetchBucketQuestions(offset = 0, pgClient) {
  try {
    const query = `SELECT 
                        q.id, 
                        q.client_id, 
                        q.question_text, 
                        q.reference, 
                        q.bucket_id,
                        b.course_id 
                    FROM 
                        questions q
                    LEFT JOIN buckets b ON q.bucket_id = b.id
                    LEFT JOIN courses c ON b.course_id  = c.id
                    WHERE 
                        q.deleted_at IS NULL 
                        AND (q.is_custom_question = FALSE OR q.is_custom_question IS NULL)
                    ORDER BY 
                        q.created_at LIMIT $1 OFFSET $2;`

    const res = await pgClient.query(query, [BATCH_SIZE, offset])
    return res.rows
  } catch (err) {
    console.error("Error fetching questions from PostgreSQL:", err)
    throw err
  }
}
