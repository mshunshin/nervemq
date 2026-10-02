-- The message's AWSTraceHeader system attribute (X-Ray format): the one the
-- sender set, else its request's X-Amzn-Trace-Id header, as AWS stores it.
-- NULL when neither was sent.
alter table messages add column aws_trace_header text;
