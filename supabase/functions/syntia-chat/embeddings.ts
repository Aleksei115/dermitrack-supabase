import { GCP_PROJECT_ID, EMBEDDING_MODEL, EMBEDDING_DIMENSION } from "./constants.ts";
import { getAccessToken } from "./vertex-auth.ts";

export async function generateEmbedding(
  text: string,
  taskType: "RETRIEVAL_QUERY" | "RETRIEVAL_DOCUMENT" = "RETRIEVAL_QUERY"
): Promise<number[]> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/us-central1/publishers/google/models/${EMBEDDING_MODEL}:predict`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      instances: [{ content: text, task_type: taskType }],
      parameters: { outputDimensionality: EMBEDDING_DIMENSION },
    }),
  });

  if (!res.ok) {
    throw new Error(`Embedding API error (${res.status}): ${await res.text()}`);
  }

  const data = await res.json();
  return data.predictions[0].embeddings.values;
}
