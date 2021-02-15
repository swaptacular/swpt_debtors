swpt_debtors
============

Swaptacular service that manages debtors

This service implements a `messaging protocol`_ client. The
deliverables are two docker images: the app-image, and the
swagger-ui-image. Both images are generated from the project's
`Dockerfile`_. The app-image is the debtor managing service. The
swagger-ui-image is a simple Swagger UI cleint for the service. To
find out what processes can be spawned from the generated app-image,
see the `entrypoint`_. For the available configuration options, see
the `example file`_.


.. _`messaging protocol`: https://github.com/epandurski/swpt_accounts/blob/master/protocol.rst
.. _Dockerfile: Dockerfile
.. _entrypoint: docker/entrypoint.sh
.. _`example file`: docker-compose-all.yml


How to run it
-------------

1. Install `Docker`_ and `Docker Compose`_.

2. To create an *.env* file with reasonable defalut values, run this
   command::

     $ cp development.env .env

3. To run the unit tests, use this command::

     $ docker-compose run tests-config test

4. To run the minimal set of services needed for development, use this
   command::

     $ docker-compose up --build


How to setup a development environment
--------------------------------------

1. Install `Poetry`_.

2. Create a new `Python`_ virtual environment and activate it.

3. To install dependencies, run this command::

     $ poetry install

4. You can use ``flask run -p 5000`` to run a local web server,
   ``dramatiq tasks:protocol_broker`` to spawn local task workers, and
   ``pytest --cov=swpt_debtors --cov-report=html`` to run the tests
   and generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

To start the containers, use this command::

     $ docker-compose -f docker-compose-all.yml up --build


.. _Docker: https://docs.docker.com/
.. _Docker Compose: https://docs.docker.com/compose/
.. _RabbitMQ: https://www.rabbitmq.com/
.. _Poetry: https://poetry.eustace.io/docs/
.. _Python: https://docs.python.org/
