#!/bin/#!/usr/bin/env bash
echo "Creating pipeline sinks"

PROJECT_ID=$(gcloud config get-value project)

# GCS buckets
#TODO: Add try/catch for the first bucket since qwiklabs
gsutil mb -l US gs://streaming_vtex
gsutil mb -l US -c "COLDLINE_VTEX" gs://coldlinevtex-coldlinevtex

# BiqQuery Dataset
bq mk --location=US logs

# PubSub Topic
gcloud pubsub topics create my_streaming
