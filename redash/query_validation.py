import sqlparse
import json
import string

class QueryValidationError(Exception):
    pass

def validate_query(data_source, query):
    validator = _get_validator(data_source, query)
    if validator is None: return

    if not validator.validate():
        raise QueryValidationError(validator.error_message())


def _get_validator(data_source, query):
    if data_source.type == 'redshift' and not data_source.options.get('allow_broadcast_joins'):
        return BroadcastJoinValidator(data_source, query)


class BroadcastJoinValidator:
    DISALLOWED_JOIN_STRATEGY = 'DS_BCAST_INNER'

    def __init__(self, data_source, query):
        self.data_source = data_source
        self.query = query

    def validate(self):
        parsed_query_statements = sqlparse.parse(self.query)
        for statement in parsed_query_statements: # skip validation if a comment contains 'ALLOW_BROADCAST_JOINS'
            if filter(lambda token: type(token) is sqlparse.sql.Comment and token.match(None, 'ALLOW_BROADCAST_JOINS', regex=True), statement): return True

        explain_query = string.join(map(self._explain_statement, parsed_query_statements))
        if explain_query == '': return True

        data, error = self.data_source.query_runner.run_query(explain_query, None)
        if error is not None: return True # allow bad queries to pass through to the query task error handling

        query_plan = string.join(map(lambda row: row['QUERY PLAN'], json.loads(data)['rows']))
        return self.DISALLOWED_JOIN_STRATEGY not in data

    def error_message(self):
        return 'This query requires a broadcast join strategy, which has been disallowed for performance reasons. Did you forget to join on vhost?'

    def _explain_statement(self, statement):
        if statement.get_type() == 'SELECT':
            return 'EXPLAIN ' + sqlparse.format(str(statement), strip_comments=True)
        else:
            return ''
