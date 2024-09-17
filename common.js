export const BATCH_SIZE = 1000

export async function insertIntoMeiliSearch(questions, offset, meiliClient) {
  try {
    const index = meiliClient.index("testpaper-questions")
    const res = await index.addDocuments(questions, { primaryKey: "id" })

    await pollTaskStatus(res.taskUid, 1000, meiliClient)

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

async function pollTaskStatus(taskUid, interval = 1000, meiliClient) {
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
