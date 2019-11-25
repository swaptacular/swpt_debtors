from werkzeug.routing import BaseConverter, ValidationError

MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MIN_UINT64 = 0
MAX_UINT64 = (1 << 64) - 1
INT64_SPAN = MAX_UINT64 + 1


def convert_i64_to_u64(value: int) -> int:
    if value > MAX_INT64 or value < MIN_INT64:
        raise ValueError()
    if value >= 0:
        return value
    return value + INT64_SPAN


def convert_u64_to_i64(value: int) -> int:
    if value > MAX_UINT64 or value < MIN_UINT64:
        raise ValueError()
    if value <= MAX_INT64:
        return value
    return value - INT64_SPAN


class Int64Converter(BaseConverter):
    regex = r"\d{1,20}"

    def to_python(self, value):
        value = int(value)
        try:
            return convert_u64_to_i64(value)
        except ValueError:
            raise ValidationError()

    def to_url(self, value):
        value = int(value)
        return str(convert_i64_to_u64(value))
