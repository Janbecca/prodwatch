-- Purpose: Trace analysis result provenance (provider/model/prompt_version).
-- Adds optional columns to analysis result tables.
--
-- NOTE: SQLite ALTER TABLE ADD COLUMN is not idempotent; if a column already exists,
-- skip that statement manually.

-- post_clean_result
ALTER TABLE post_clean_result ADD COLUMN provider_name TEXT;
ALTER TABLE post_clean_result ADD COLUMN model_name TEXT;
ALTER TABLE post_clean_result ADD COLUMN prompt_version TEXT;
ALTER TABLE post_clean_result ADD COLUMN generated_at DATETIME;
ALTER TABLE post_clean_result ADD COLUMN raw_response TEXT;
ALTER TABLE post_clean_result ADD COLUMN error_message TEXT;

-- post_sentiment_result
ALTER TABLE post_sentiment_result ADD COLUMN provider_name TEXT;
ALTER TABLE post_sentiment_result ADD COLUMN model_name TEXT;
ALTER TABLE post_sentiment_result ADD COLUMN prompt_version TEXT;
ALTER TABLE post_sentiment_result ADD COLUMN generated_at DATETIME;
ALTER TABLE post_sentiment_result ADD COLUMN raw_response TEXT;
ALTER TABLE post_sentiment_result ADD COLUMN error_message TEXT;

-- post_keyword_result
ALTER TABLE post_keyword_result ADD COLUMN provider_name TEXT;
ALTER TABLE post_keyword_result ADD COLUMN model_name TEXT;
ALTER TABLE post_keyword_result ADD COLUMN prompt_version TEXT;
ALTER TABLE post_keyword_result ADD COLUMN generated_at DATETIME;
ALTER TABLE post_keyword_result ADD COLUMN raw_response TEXT;
ALTER TABLE post_keyword_result ADD COLUMN error_message TEXT;

-- post_feature_result
ALTER TABLE post_feature_result ADD COLUMN provider_name TEXT;
ALTER TABLE post_feature_result ADD COLUMN model_name TEXT;
ALTER TABLE post_feature_result ADD COLUMN prompt_version TEXT;
ALTER TABLE post_feature_result ADD COLUMN generated_at DATETIME;
ALTER TABLE post_feature_result ADD COLUMN raw_response TEXT;
ALTER TABLE post_feature_result ADD COLUMN error_message TEXT;

-- post_spam_result
ALTER TABLE post_spam_result ADD COLUMN provider_name TEXT;
ALTER TABLE post_spam_result ADD COLUMN model_name TEXT;
ALTER TABLE post_spam_result ADD COLUMN prompt_version TEXT;
ALTER TABLE post_spam_result ADD COLUMN generated_at DATETIME;
ALTER TABLE post_spam_result ADD COLUMN raw_response TEXT;
ALTER TABLE post_spam_result ADD COLUMN error_message TEXT;

