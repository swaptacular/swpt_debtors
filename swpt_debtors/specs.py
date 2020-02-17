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

DEBTOR_DOES_NOT_EXIST = {
    'description': 'The debtor does not exist.',
}

CONFLICTING_DEBTOR = {
    'description': 'A debtor with the same ID already exists.',
}

CONFLICTING_POLICY = {
    'description': 'The new policy is in conflict with the old one.',
}

TRANSFER_DOES_NOT_EXIST = {
    'description': 'The transfer entry does not exist.',
}

TRANSFER_CONFLICT = {
    'description': 'A different transfer entry with the same UUID already exists.',
}

TRANSFER_UPDATE_CONFLICT = {
    'description': 'The requested transfer update is not possible.',
}

TOO_MANY_TRANSFERS = {
    'description': 'Too many issuing transfers.',
}

TOO_MANY_POLICY_CHANGES = {
    'description': 'Too many policy changes.',
}

TRANSFER_EXISTS = {
    'description': 'The same transfer entry already exists.',
    'headers': LOCATION_HEADER,
}
