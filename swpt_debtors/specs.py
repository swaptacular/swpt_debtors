"""JSON snippets to be included in the OpenAPI specification file."""

DEBTOR_ID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'integer',
        'format': 'uint64',
        'minimum': 0,
        'maximum': (1 << 64) - 1,
    },
}

TRANSFER_UUID = {
    'in': 'path',
    'name': 'transferUuid',
    'required': True,
    'description': "The transfer's UUID",
    'schema': {
        'type': 'string',
        'format': 'uuid',
    },
}

LOCATION_HEADER = {
    'Location': {
        'description': 'The URI of the entry.',
        'schema': {
            'type': 'string',
            'format': 'uri',
        },
    },
}

ERROR_CONTENT = {
    'application/json': {
        'schema': {
            'type': 'object',
            'properties': {
                'code': {
                    'type': 'integer',
                    'format': 'int32',
                    'description': 'Error code',
                },
                'errors': {
                    'type': 'object',
                    'description': 'Errors',
                },
                'status': {
                    'type': 'string',
                    'description': 'Error name',
                },
                'message': {
                    'type': 'string',
                    'description': 'Error message',
                }
            }
        }
    }
}

DEBTOR_DOES_NOT_EXIST = {
    'description': 'The debtor does not exist.',
    'content': ERROR_CONTENT,
}

CONFLICTING_DEBTOR = {
    'description': 'A debtor with the same ID already exists.',
    'content': ERROR_CONTENT,
}

CONFLICTING_POLICY = {
    'description': 'The new policy is in conflict with the old one.',
    'content': ERROR_CONTENT,
}

TRANSFER_DOES_NOT_EXIST = {
    'description': 'The transfer entry does not exist.',
    'content': ERROR_CONTENT,
}

TRANSFER_CONFLICT = {
    'description': 'A different transfer entry with the same UUID already exists.',
    'content': ERROR_CONTENT,
}

TRANSFER_UPDATE_CONFLICT = {
    'description': 'The requested transfer update is not possible.',
    'content': ERROR_CONTENT,
}

TOO_MANY_TRANSFERS = {
    'description': 'Too many issuing transfers.',
    'content': ERROR_CONTENT,
}

TOO_MANY_POLICY_CHANGES = {
    'description': 'Too many policy changes.',
    'content': ERROR_CONTENT,
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

SCOPE_ACCESS = [
    {'oauth2': ['access']},
]

SCOPE_ACTIVATE = [
    {'oauth2': ['activate']},
]

SCOPE_DEACTIVATE = [
    {'oauth2': ['deactivate']},
]

API_DESCRIPTION = """This API can be used to:
1. Obtain public information about debtors and create new debtors.
2. Change individual debtor's policies.
3. Make credit-issuing transfers.
"""

API_SPEC_OPTIONS = {
    'info': {
        'description': API_DESCRIPTION,
    },
    'servers': [
        {'url': '/'},
        {'url': '$API_ROOT', 'description': 'Production server (uses live data)'},
    ],
    'consumes': ['application/json'],
    'produces': ['application/json'],
    'components': {
        'securitySchemes': {
            'oauth2': {
                'type': 'oauth2',
                'description': 'This API uses OAuth 2. [More info](https://oauth.net/2/).',
                'flows': {
                    'authorizationCode': {
                        'authorizationUrl': '$OAUTH2_AUTHORIZATION_URL',
                        'tokenUrl': '$OAUTH2_TOKEN_URL',
                        'refreshUrl': '$OAUTH2_REFRESH_URL',
                        'scopes': {
                            'access': 'read-write access',
                        },
                    },
                    'clientCredentials': {
                        'tokenUrl': '$OAUTH2_TOKEN_URL',
                        'refreshUrl': '$OAUTH2_REFRESH_URL',
                        'scopes': {
                            'access': 'read-write access',
                            'activate': 'activate new debtors',
                            'deactivate': 'deactivate existing debtors',
                        },
                    },
                },
            },
        },
    },
}
