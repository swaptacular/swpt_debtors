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
    "description": "The debtor has not been found.",
}

DEBTOR_EXISTS = {
    "description": "The debtor has been found.",
    'headers': LOCATION_HEADER,
}

CONFLICTING_DEBTOR = {
    'description': 'A debtor with the same ID already exists.',
    'content': ERROR_CONTENT,
}

CONFLICTING_POLICY = {
    'description': 'The new policy is in conflict with the old one.',
    'content': ERROR_CONTENT,
}

TRANSFER_CONFLICT = {
    'description': 'A different transfer entry with the same UUID already exists.',
    'content': ERROR_CONTENT,
}

TRANSFER_CANCELLATION_FAILURE = {
    'description': 'The transfer can not be canceled.',
    'content': ERROR_CONTENT,
}

FORBIDDEN_OPERATION = {
    'description': 'Forbidden operation.',
    'content': ERROR_CONTENT,
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

SCOPE_ACCESS_READONLY = [
    {'oauth2': ['access.readonly']},
]

SCOPE_ACCESS_MODIFY = [
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
                            'access.readonly': 'read-only access',
                            'access': 'read-write access',
                        },
                    },
                    'clientCredentials': {
                        'tokenUrl': '$OAUTH2_TOKEN_URL',
                        'refreshUrl': '$OAUTH2_REFRESH_URL',
                        'scopes': {
                            'access.readonly': 'read-only access',
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

DEBTORS_LIST_EXAMPLE = {
    'type': 'DebtorsList',
    'uri': '/debtors/.list',
    'itemsType': 'ObjectReference',
    'first': '/debtors/9223372036854775808/enumerate',
}

DEBTOR_LINKS_EXAMPLE = {
    'uri': '/debtors/2/enumerate',
    'type': 'ObjectReferencesPage',
    'items': [
        {'uri': '/debtors/2/'},
        {'uri': '/debtors/5/'},
        {'uri': '/debtors/11/'},
    ],
    'next': '/debtors/12/enumerate',
}
