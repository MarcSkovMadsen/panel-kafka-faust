# Building a Fraud Detection App with HoloViz Panel, Apache Kafka and Faust

WORK IN PROGRESS

This application is inspired by the blog post [How to build a real-time fraud detection pipeline using Faust and MLFlow](https://towardsdatascience.com/how-to-build-a-real-time-fraud-detection-pipeline-using-faust-and-mlflow-24e787dd51fa). The Code is [here](https://github.com/BogdanCojocar/medium-articles/tree/master/realtime_fraud_detection).

You can use the as a template or as inspiration for building HoloViz Panel apps that are event driven and supports streaming.

## Confluent Kafka

Prerequisites

- A Confluent Cloud Cluster
- Knowledge at a level of [Python: Code Example for Apache Kafka](https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/python.html?utm_source=github&utm_medium=demo&utm_campaign=ch.examples_type.community_content.clients-ccloud)
    - [Repo](https://github.com/confluentinc/examples/tree/latest/clients/cloud/python#overview)

## Create the Conda Environment

conda create -c pyviz -c conda-forge -n panel-kafka-faust panel holoviz hvplot

https://github.com/simplesteph/kafka-stack-docker-compose

https://github.com/confluentinc/examples/tree/latest/clients/cloud/python#overview

https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#ce-docker-quickstart