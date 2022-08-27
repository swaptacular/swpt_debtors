Swaptacular "Debtors Agent" reference implementation
====================================================

This project implements a [Swaptacular] "Debtors Agent" node. The
deliverables are two [docker images]: the *app-image*, and the
*swagger-ui-image*. Both images are generated from the project's
[Dockerfile](../master/Dockerfile).

* The `app-image` contains all the necessary services. The most
  important provided service is the [Simple Issuing Web API]. This is
  a server Web API, which allows debtors to issue new currency tokens
  into existence. Normally, in order to "talk" to the debtors agent,
  the currency issuers will use a [currency issuing client
  application].

* The `swagger-ui-image` is a simple [Swagger UI] cleint for the
  server Web API, mainly useful for testing.

**Note:** This implementation uses [JSON Serialization for the
Swaptacular Messaging Protocol].


Dependencies
------------

Containers started from the *app-image* must have access to the
following servers:

1. [PostgreSQL] server instance, which stores debtors' data.

2. [RabbitMQ] server instance, which acts as broker for [Swaptacular
   Messaging Protocol] (SMP) messages.

   A [RabbitMQ queue] must be configured on the broker instance, so
   that all incoming SMP messages for the debtors stored on the
   PostgreSQL server instance, are routed to this queue.

   Also, a [RabbitMQ exchange] named **`debtors_out`** must be
   configured on the broker instance. This exchange is for messages
   that must be send to the accounting authority. The routing key will
   be an empty string.

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them. However,
   this works only for the most basic setup.

3. [OAuth 2.0] authorization server, which authorizes clients'
   requests to the [Simple Issuing Web API]. There is a plethora of
   popular Oauth 2.0 server implementations. Normally, they maintain
   their own user database, and go together with UI for user
   registration, login, and authorization consent.

To increase security and performance, it is highly recommended that
you configure reverse Web-proxy server(s) (like [nginx]) between your
clients and your "Simple Issuing Web API" servers. In addition, this
approach allows different debtors to be located on different database
servers (sharding).


Configuration
-------------

The behavior of the running container can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# The debtors agent will be responsible only for debtor IDs between
# "$MIN_DEBTOR_ID" and "$MAX_DEBTOR_ID".
MIN_DEBTOR_ID=4294967296
MAX_DEBTOR_ID=8589934591

# The specified number of processes ("$WEBSERVER_PROCESSES") will be
# spawned to handle "Simple Issuing Web API" requests (default 1),
# each process will run "$WEBSERVER_THREADS" threads in parallel
# (default 3). The container will listen for "Simple Issuing Web API"
# requests on port "$WEBSERVER_PORT" (default 80).
WEBSERVER_PROCESSES=2
WEBSERVER_THREADS=10
WEBSERVER_PORT=8003

# Requests to the "Simple Issuing Web API" are protected by an OAuth
# 2.0 authorization server. With every request, the client (a Web
# browser, for example) presents a token, and to verify the validity
# of the token, internally, a request is made to the OAuth 2.0
# authorization server. This is called "token introspection". This
# variable sets the URL at which internal token introspection requests
# will be send.
OAUTH2_INTROSPECT_URL=http://localhost:4445/oauth2/introspect

# Connection string for a PostgreSQL database server to connect to.
POSTGRES_URL=postgresql://swpt_debtors:swpt_debtors@localhost:5435/test

# Parameters for the communication with the RabbitMQ server which is
# responsible for brokering SMP messages. The container will connect
# to "$PROTOCOL_BROKER_URL" (default
# "amqp://guest:guest@localhost:5672"), will consume messages from the
# queue named "$PROTOCOL_BROKER_QUEUE" (default "swpt_debtors"),
# prefetching at most "$PROTOCOL_BROKER_PREFETCH_COUNT" messages at
# once (default 1). The specified number of processes
# ("$PROTOCOL_BROKER_PROCESSES") will be spawned to consume and
# process messages (default 1), each process will run
# "$PROTOCOL_BROKER_THREADS" threads in parallel (default 1). Note
# that PROTOCOL_BROKER_PROCESSES can be set to 0, in which case, the
# container will not consume any messages from the queue.
PROTOCOL_BROKER_URL=amqp://guest:guest@localhost:5672
PROTOCOL_BROKER_QUEUE=swpt_debtors
PROTOCOL_BROKER_PROCESSES=1
PROTOCOL_BROKER_THREADS=3
PROTOCOL_BROKER_PREFETCH_COUNT=10

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


Available commands
------------------

The [entrypoint](../master/docker/entrypoint.sh) of the docker
container allows you to execute the following *documented commands*:

* `all`

  Starts all the necessary services in the container. Also, this is
  the command that will be executed if no arguments are passed to the
  entrypoint.

  **IMPORTANT NOTE: For each database instance, you must start exactly
  one container with this command.**

* `configure`

  Initializes a new empty PostgreSQL database.

  **IMPORTANT NOTE: This command has to be run only once (at the
  beginning), but running it multiple times should not do any harm.**

* `webserver`

  Starts only the "Simple Issuing Web API" server. This command allows
  you to start as many additional dedicated web servers as necessary,
  to handle the incoming load.

* `consume_messages`

  Starts only the processes that consume SMP messages. This command
  allows you to start as many additional dedicated SMP message
  processors as necessary, to handle the incoming load.


This [docker-compose example](../master/docker-compose-all.yml) shows
how to use the generated docker image, along with the PostgerSQL
server, and the RabbitMQ server.


How to run the tests
--------------------

1.  Install [Docker Engine] and [Docker Compose].

2.  To create an *.env* file with reasonable defalut values, run this
    command:

        $ cp development.env .env

3.  To run the unit tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-config test

4.  To run the minimal set of services needed for development, use
    this command:

        $ docker-compose up --build

    This will start its own PostgreSQL server instance in a docker
    container, but will rely on being able to connect to a RabbitMQ
    server instance at "amqp://guest:guest@localhost:5672".

    Note that because the RabbitMQ "guest" user [can only connect from
    localhost], you should either explicitly allow the "guest" user to
    connect from anywhere, or create a new RabbitMQ user, and change
    the RabbitMQ connection URLs accordingly (`PROTOCOL_BROKER_URL` in
    the *.env* file).


How to setup a development environment
--------------------------------------

1.  Install [Poetry](https://poetry.eustace.io/docs/).

2.  Create a new [Python](https://docs.python.org/) virtual
    environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install

4.  You can use `flask run -p 5000` to run a local web server, and
    `pytest --cov=swpt_debtors --cov-report=html` to run the tests and
    generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

To start the "Debtors Agent" server, along with a Swagger UI client, a
PostgerSQL server, a RabbitMQ server, an OAuth 2.0 authorization
server, and a HTTP-proxy server, use this command:

    $ docker-compose -f docker-compose-all.yml up --build

Then, you can open a browser window at
https://localhost:44302/debtors-swagger-ui/ and use client ID
`swagger-ui`, and client secret `swagger-ui` to authorize Swagger UI
to use the server API. In this testing environment, user registration
emails will be sent to a fake email server, whose messages can be read
at http://localhost:8026/



[Swaptacular]: https://swaptacular.github.io/overview
[docker images]: https://www.geeksforgeeks.org/what-is-docker-images/
[Simple Issuing Web API]: https://swaptacular.org/public/docs/swpt_debtors/redoc.html
[currency issuing client application]: https://github.com/swaptacular/swpt_debtors_ui
[Swagger UI]: https://swagger.io/tools/swagger-ui/
[JSON Serialization for the Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol-json.rst
[PostgreSQL]: https://www.postgresql.org/
[Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
[RabbitMQ]: https://www.rabbitmq.com/
[RabbitMQ queue]: https://www.cloudamqp.com/blog/part1-rabbitmq-for-beginners-what-is-rabbitmq.html
[RabbitMQ exchange]: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
[OAuth 2.0]: https://oauth.net/2/
[nginx]: https://en.wikipedia.org/wiki/Nginx
[Docker Engine]: https://docs.docker.com/engine/
[Docker Compose]: https://docs.docker.com/compose/
[Poetry]: https://poetry.eustace.io/docs/
[Python]: https://docs.python.org/
[can only connect from localhost]: https://www.rabbitmq.com/access-control.html#loopback-users
