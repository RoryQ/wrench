CREATE MODEL custom_ai_model2 INPUT (
  prompt STRING(MAX)
) OUTPUT (
  content STRING(MAX)
) REMOTE OPTIONS (
  endpoint = '//aiplatform.googleapis.com/projects/${PROJECT_ID}/locations/us-central1/publishers/google/models/${INSTANCE_ID}-${DATABASE_ID}-model',
  default_batch_size = 1
); 
