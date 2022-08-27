"""JSON snippets to be included in the OpenAPI specification file."""

DEBTOR_ID = {
    'in': 'path',
    'name': 'debtorId',
    'required': True,
    'description': "The debtor's ID",
    'schema': {
        'type': 'string',
        'pattern': '^[0-9A-Za-z_=-]{1,64}$',
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

DOC_ID = {
    'in': 'path',
    'name': 'documentId',
    'required': True,
    'description': "The document's ID",
    'schema': {
        'type': 'string',
        'pattern': '^[0-9A-Za-z_=-]{1,64}$',
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

UPDATE_CONFLICT = {
    'description': 'Conflicting update attempts.',
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

DOCUMENT_IS_TOO_BIG = {
    'description': 'The document is too big.',
    'content': ERROR_CONTENT,
}

DOCUMENT_CONTENT = {
    'content': {
        'text/plain': {
            'example': 'This is an example document.',
        },
        '*/*': {},
    }
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}

DEBTOR_INFO_EXISTS = {
    "description": "The debtor's public info document has been found.",
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

API_DESCRIPTION = """Since interchangeability of client applications for currency
issuing is not of critical importance, Swaptacular does not make
recommendations about the Issuing Web API. The current reference
implementation uses this `Simple Issuing Web API`.

This API is organized in four separate sections: **admin**,
**debtors**, **transfers**, **documents**.
"""

API_SPEC_OPTIONS = {
    'info': {
        'description': API_DESCRIPTION,
    },
    'servers': [
        {'url': '$API_ROOT'},
        {'url': '/'},
    ],
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
        {'uri': '/debtors/1/'},
        {'uri': '/debtors/5/'},
        {'uri': '/debtors/11/'},
    ],
    'next': '/debtors/12/enumerate',
}
