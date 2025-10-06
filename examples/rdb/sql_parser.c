#include "sql_parser.h"
#include <ctype.h>
#include <stdarg.h>
#include <strings.h>  /* for strcasecmp */

/* Forward declarations */
static rdb_statement_t* sql_parse_create_statement(sql_parser_t *parser);
static rdb_statement_t* sql_parse_drop_statement(sql_parser_t *parser);
static rdb_statement_t* sql_parse_create_index_simple(sql_parser_t *parser, rdb_statement_t *stmt);
static int sql_parse_string_literal(sql_parser_t *parser, char quote_char);
static int sql_parse_number(sql_parser_t *parser);
static int sql_parse_identifier(sql_parser_t *parser);
static int sql_parse_operator(sql_parser_t *parser);
static bool sql_is_operator_char(char c);

/* Parser creation and destruction */
sql_parser_t* sql_parser_create(const char *sql) {
    if (!sql) return NULL;
    
    sql_parser_t *parser = malloc(sizeof(sql_parser_t));
    if (!parser) return NULL;
    
    parser->sql = malloc(strlen(sql) + 1);
    if (!parser->sql) {
        free(parser);
        return NULL;
    }
    
    strcpy(parser->sql, sql);
    parser->pos = 0;
    parser->length = strlen(sql);
    parser->has_error = false;
    parser->error_message[0] = '\0';
    
    /* Initialize current token */
    parser->current_token.type = SQL_TOKEN_UNKNOWN;
    parser->current_token.value = NULL;
    parser->current_token.length = 0;
    parser->current_token.position = 0;
    
    return parser;
}

void sql_parser_destroy(sql_parser_t *parser) {
    if (!parser) return;
    
    if (parser->sql) {
        free(parser->sql);
    }
    
    if (parser->current_token.value) {
        free(parser->current_token.value);
    }
    
    free(parser);
}

/* Tokenization */
void sql_parser_skip_whitespace(sql_parser_t *parser) {
    while (parser->pos < parser->length && sql_is_whitespace(parser->sql[parser->pos])) {
        parser->pos++;
    }
}

char sql_parser_peek_char(sql_parser_t *parser) {
    if (parser->pos >= parser->length) return '\0';
    return parser->sql[parser->pos];
}

char sql_parser_next_char(sql_parser_t *parser) {
    if (parser->pos >= parser->length) return '\0';
    return parser->sql[parser->pos++];
}

int sql_parser_next_token(sql_parser_t *parser) {
    if (!parser) return -1;
    
    /* Free previous token value */
    if (parser->current_token.value) {
        free(parser->current_token.value);
        parser->current_token.value = NULL;
    }
    
    sql_parser_skip_whitespace(parser);
    
    if (parser->pos >= parser->length) {
        parser->current_token.type = SQL_TOKEN_EOF;
        parser->current_token.value = NULL;
        parser->current_token.length = 0;
        parser->current_token.position = parser->pos;
        return 0;
    }
    
    char c = parser->sql[parser->pos];
    parser->current_token.position = parser->pos;
    
    /* Handle string literals */
    if (c == '\'' || c == '"') {
        return sql_parse_string_literal(parser, c);
    }
    
    /* Handle numbers */
    if (isdigit(c) || c == '-') {
        return sql_parse_number(parser);
    }
    
    /* Handle identifiers and keywords */
    if (sql_is_identifier_char(c)) {
        return sql_parse_identifier(parser);
    }
    
    /* Handle operators */
    if (sql_is_operator_char(c)) {
        return sql_parse_operator(parser);
    }
    
    /* Handle punctuation */
    if (sql_is_punctuation(c)) {
        parser->current_token.type = SQL_TOKEN_PUNCTUATION;
        parser->current_token.value = malloc(2);
        if (!parser->current_token.value) return -1;
        
        parser->current_token.value[0] = c;
        parser->current_token.value[1] = '\0';
        parser->current_token.length = 1;
        parser->pos++;
        return 0;
    }
    
    /* Unknown character */
    parser->current_token.type = SQL_TOKEN_UNKNOWN;
    parser->current_token.value = malloc(2);
    if (!parser->current_token.value) return -1;
    
    parser->current_token.value[0] = c;
    parser->current_token.value[1] = '\0';
    parser->current_token.length = 1;
    parser->pos++;
    
    sql_parser_set_error(parser, "Unknown character: '%c'", c);
    return -1;
}

int sql_parse_string_literal(sql_parser_t *parser, char quote_char) {
    parser->current_token.type = SQL_TOKEN_STRING;
    parser->pos++; /* Skip opening quote */
    
    size_t start = parser->pos;
    while (parser->pos < parser->length && parser->sql[parser->pos] != quote_char) {
        parser->pos++;
    }
    
    if (parser->pos >= parser->length) {
        sql_parser_set_error(parser, "Unterminated string literal");
        return -1;
    }
    
    size_t length = parser->pos - start;
    parser->current_token.value = malloc(length + 1);
    if (!parser->current_token.value) return -1;
    
    strncpy(parser->current_token.value, parser->sql + start, length);
    parser->current_token.value[length] = '\0';
    parser->current_token.length = length;
    
    parser->pos++; /* Skip closing quote */
    return 0;
}

int sql_parse_number(sql_parser_t *parser) {
    parser->current_token.type = SQL_TOKEN_NUMBER;
    size_t start = parser->pos;
    
    /* Handle negative numbers */
    if (parser->sql[parser->pos] == '-') {
        parser->pos++;
    }
    
    /* Parse integer part */
    while (parser->pos < parser->length && isdigit(parser->sql[parser->pos])) {
        parser->pos++;
    }
    
    /* Parse decimal part */
    if (parser->pos < parser->length && parser->sql[parser->pos] == '.') {
        parser->pos++;
        while (parser->pos < parser->length && isdigit(parser->sql[parser->pos])) {
            parser->pos++;
        }
    }
    
    size_t length = parser->pos - start;
    parser->current_token.value = malloc(length + 1);
    if (!parser->current_token.value) return -1;
    
    strncpy(parser->current_token.value, parser->sql + start, length);
    parser->current_token.value[length] = '\0';
    parser->current_token.length = length;
    
    return 0;
}

int sql_parse_identifier(sql_parser_t *parser) {
    size_t start = parser->pos;
    
    while (parser->pos < parser->length && sql_is_identifier_char(parser->sql[parser->pos])) {
        parser->pos++;
    }
    
    size_t length = parser->pos - start;
    parser->current_token.value = malloc(length + 1);
    if (!parser->current_token.value) return -1;
    
    strncpy(parser->current_token.value, parser->sql + start, length);
    parser->current_token.value[length] = '\0';
    parser->current_token.length = length;
    
    /* Check if it's a keyword */
    if (sql_is_keyword(parser->current_token.value)) {
        parser->current_token.type = SQL_TOKEN_KEYWORD;
    } else {
        parser->current_token.type = SQL_TOKEN_IDENTIFIER;
    }
    
    return 0;
}

int sql_parse_operator(sql_parser_t *parser) {
    parser->current_token.type = SQL_TOKEN_OPERATOR;
    size_t start = parser->pos;
    
    char c1 = parser->sql[parser->pos];
    char c2 = (parser->pos + 1 < parser->length) ? parser->sql[parser->pos + 1] : '\0';
    
    /* Check for two-character operators */
    if ((c1 == '=' && c2 == '=') ||
        (c1 == '!' && c2 == '=') ||
        (c1 == '<' && c2 == '=') ||
        (c1 == '>' && c2 == '=')) {
        parser->pos += 2;
    } else {
        parser->pos++;
    }
    
    size_t length = parser->pos - start;
    parser->current_token.value = malloc(length + 1);
    if (!parser->current_token.value) return -1;
    
    strncpy(parser->current_token.value, parser->sql + start, length);
    parser->current_token.value[length] = '\0';
    parser->current_token.length = length;
    
    return 0;
}

/* Helper functions */
bool sql_is_keyword(const char *word) {
    const char *keywords[] = {
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INDEX",
        "DROP", "INT", "FLOAT", "VARCHAR", "TEXT", "BOOLEAN",
        "PRIMARY", "KEY", "UNIQUE", "NOT", "NULL", "DEFAULT",
        "AND", "OR", "ORDER", "BY", "LIMIT", "OFFSET", "JOIN",
        "INNER", "LEFT", "RIGHT", "FULL", "ON", "FOREIGN",
        "REFERENCES", "CASCADE", "CONSTRAINT", "BEGIN", "COMMIT",
        "ROLLBACK", "TRANSACTION", "AUTOCOMMIT", "ISOLATION", "LEVEL",
        "READ", "UNCOMMITTED", "COMMITTED", "REPEATABLE", "SERIALIZABLE"
    };
    
    for (size_t i = 0; i < sizeof(keywords) / sizeof(keywords[0]); i++) {
        if (strcasecmp(word, keywords[i]) == 0) {
            return true;
        }
    }
    return false;
}

bool sql_is_operator(const char *op) {
    const char *operators[] = {
        "=", "!=", "<", ">", "<=", ">=", "LIKE", "IS", "IN"
    };
    
    for (size_t i = 0; i < sizeof(operators) / sizeof(operators[0]); i++) {
        if (strcmp(op, operators[i]) == 0) {
            return true;
        }
    }
    return false;
}

bool sql_is_punctuation(char c) {
    return c == ',' || c == ';' || c == '(' || c == ')' || c == '.' || c == '*';
}

bool sql_is_whitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

bool sql_is_identifier_char(char c) {
    return isalnum(c) || c == '_';
}

bool sql_is_operator_char(char c) {
    return c == '=' || c == '!' || c == '<' || c == '>' || c == '~';
}

sql_keyword_t sql_get_keyword(const char *keyword) {
    const char *keywords[] = {
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INDEX",
        "DROP", "INT", "FLOAT", "VARCHAR", "TEXT", "BOOLEAN",
        "PRIMARY", "KEY", "UNIQUE", "NOT", "NULL", "DEFAULT",
        "AND", "OR", "ORDER", "BY", "LIMIT", "OFFSET", "JOIN",
        "INNER", "LEFT", "RIGHT", "FULL", "ON", "FOREIGN",
        "REFERENCES", "CASCADE", "CONSTRAINT", "BEGIN", "COMMIT",
        "ROLLBACK", "TRANSACTION", "AUTOCOMMIT", "ISOLATION", "LEVEL",
        "READ", "UNCOMMITTED", "COMMITTED", "REPEATABLE", "SERIALIZABLE"
    };
    
    for (size_t i = 0; i < sizeof(keywords) / sizeof(keywords[0]); i++) {
        if (strcasecmp(keyword, keywords[i]) == 0) {
            return i + 1;
        }
    }
    return 0;
}

sql_operator_t sql_get_operator(const char *op) {
    if (strcmp(op, "=") == 0) return SQL_OP_EQUAL;
    if (strcmp(op, "!=") == 0) return SQL_OP_NOT_EQUAL;
    if (strcmp(op, "<") == 0) return SQL_OP_LESS_THAN;
    if (strcmp(op, ">") == 0) return SQL_OP_GREATER_THAN;
    if (strcmp(op, "<=") == 0) return SQL_OP_LESS_EQUAL;
    if (strcmp(op, ">=") == 0) return SQL_OP_GREATER_EQUAL;
    if (strcmp(op, "LIKE") == 0) return SQL_OP_LIKE;
    if (strcmp(op, "IS") == 0) return SQL_OP_IS;
    if (strcmp(op, "IN") == 0) return SQL_OP_IN;
    return 0;
}

/* Statement parsing */
rdb_statement_t* sql_parse_statement(sql_parser_t *parser) {
    if (!parser) return NULL;
    
    if (sql_parser_next_token(parser) != 0) {
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
        sql_parser_set_error(parser, "Expected SQL keyword at start of statement");
        return NULL;
    }
    
    sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
    
    switch (keyword) {
        case SQL_KW_CREATE:
            return sql_parse_create_statement(parser);
        case SQL_KW_DROP:
            return sql_parse_drop_statement(parser);
        case SQL_KW_INSERT:
            return sql_parse_insert(parser);
        case SQL_KW_SELECT:
            return sql_parse_select(parser);
        case SQL_KW_UPDATE:
            return sql_parse_update(parser);
        case SQL_KW_DELETE:
            return sql_parse_delete(parser);
        case SQL_KW_BEGIN:
            return sql_parse_begin_transaction(parser);
        case SQL_KW_COMMIT:
            return sql_parse_commit_transaction(parser);
        case SQL_KW_ROLLBACK:
            return sql_parse_rollback_transaction(parser);
        default:
            sql_parser_set_error(parser, "Unsupported SQL statement type");
            return NULL;
    }
}


rdb_statement_t* sql_parse_create_statement(sql_parser_t *parser) {
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    /* Parse CREATE TABLE or CREATE INDEX */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
        sql_parser_set_error(parser, "Expected TABLE or INDEX after CREATE");
        free(stmt);
        return NULL;
    }
    
    sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
    if (keyword == SQL_KW_TABLE) {
        stmt->type = RDB_STMT_CREATE_TABLE;
        return sql_parse_create_table(parser, stmt);
    } else if (keyword == SQL_KW_INDEX) {
        stmt->type = RDB_STMT_CREATE_INDEX;
        return sql_parse_create_index_simple(parser, stmt);
    } else {
        sql_parser_set_error(parser, "Expected TABLE or INDEX after CREATE");
        free(stmt);
        return NULL;
    }
}

rdb_statement_t* sql_parse_create_table(sql_parser_t *parser, rdb_statement_t *stmt) {
    /* Parse table name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->table_name, parser->current_token.value, sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    
    /* Parse opening parenthesis */
    if (!sql_parser_expect_punctuation(parser, '(')) {
        free(stmt);
        return NULL;
    }
    
    /* Parse column definitions */
    stmt->columns = fi_array_create(16, sizeof(rdb_column_t*));
    if (!stmt->columns) {
        free(stmt);
        return NULL;
    }
    
    while (sql_parser_next_token(parser) == 0 && 
           parser->current_token.type != SQL_TOKEN_PUNCTUATION) {
        
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION && 
            parser->current_token.value[0] == ')') {
            break;
        }
        
        rdb_column_t *column = malloc(sizeof(rdb_column_t));
        if (!column) {
            fi_array_destroy(stmt->columns);
            free(stmt);
            return NULL;
        }
        
        if (sql_parse_column_definition(parser, column) != 0) {
            free(column);
            fi_array_destroy(stmt->columns);
            free(stmt);
            return NULL;
        }
        
        fi_array_push(stmt->columns, &column);
        
        /* Check for comma or closing parenthesis */
        if (sql_parser_next_token(parser) != 0) break;
        
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION) {
            if (parser->current_token.value[0] == ')') {
                break;
            } else if (parser->current_token.value[0] != ',') {
                sql_parser_set_error(parser, "Expected comma or closing parenthesis");
                free(column);
                fi_array_destroy(stmt->columns);
                free(stmt);
                return NULL;
            }
        }
    }
    
    return stmt;
}

/* Placeholder implementations for missing functions */
static rdb_statement_t* sql_parse_drop_statement(sql_parser_t *parser) {
    /* TODO: Implement DROP statement parsing */
    sql_parser_set_error(parser, "DROP statement parsing not implemented");
    return NULL;
}

rdb_statement_t* sql_parse_insert(sql_parser_t *parser) {
    /* TODO: Implement INSERT statement parsing */
    sql_parser_set_error(parser, "INSERT statement parsing not implemented");
    return NULL;
}

rdb_statement_t* sql_parse_select(sql_parser_t *parser) {
    /* TODO: Implement SELECT statement parsing */
    sql_parser_set_error(parser, "SELECT statement parsing not implemented");
    return NULL;
}

rdb_statement_t* sql_parse_update(sql_parser_t *parser) {
    /* TODO: Implement UPDATE statement parsing */
    sql_parser_set_error(parser, "UPDATE statement parsing not implemented");
    return NULL;
}

rdb_statement_t* sql_parse_delete(sql_parser_t *parser) {
    /* TODO: Implement DELETE statement parsing */
    sql_parser_set_error(parser, "DELETE statement parsing not implemented");
    return NULL;
}

static rdb_statement_t* sql_parse_create_index_simple(sql_parser_t *parser, rdb_statement_t *stmt) {
    /* TODO: Implement CREATE INDEX statement parsing */
    sql_parser_set_error(parser, "CREATE INDEX statement parsing not implemented");
    free(stmt);
    return NULL;
}

int sql_parse_column_definition(sql_parser_t *parser, rdb_column_t *column) {
    /* Parse column name */
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected column name");
        return -1;
    }
    
    strncpy(column->name, parser->current_token.value, sizeof(column->name) - 1);
    column->name[sizeof(column->name) - 1] = '\0';
    
    /* Parse data type */
    if (sql_parser_next_token(parser) != 0) {
        sql_parser_set_error(parser, "Expected data type");
        return -1;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
        sql_parser_set_error(parser, "Expected data type keyword");
        return -1;
    }
    
    sql_keyword_t type_keyword = sql_get_keyword(parser->current_token.value);
    switch (type_keyword) {
        case SQL_KW_INT:
            column->type = RDB_TYPE_INT;
            break;
        case SQL_KW_FLOAT:
            column->type = RDB_TYPE_FLOAT;
            break;
        case SQL_KW_VARCHAR:
            column->type = RDB_TYPE_VARCHAR;
            column->max_length = 255; /* Default length */
            break;
        case SQL_KW_TEXT:
            column->type = RDB_TYPE_TEXT;
            break;
        case SQL_KW_BOOLEAN:
            column->type = RDB_TYPE_BOOLEAN;
            break;
        default:
            sql_parser_set_error(parser, "Unknown data type: %s", parser->current_token.value);
            return -1;
    }
    
    /* Initialize default values */
    column->nullable = true;
    column->primary_key = false;
    column->unique = false;
    column->default_value[0] = '\0';
    
    /* Parse column constraints */
    while (sql_parser_next_token(parser) == 0) {
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION) {
            break;
        }
        
        if (parser->current_token.type == SQL_TOKEN_KEYWORD) {
            sql_keyword_t constraint = sql_get_keyword(parser->current_token.value);
            switch (constraint) {
                case SQL_KW_PRIMARY:
                    if (sql_parser_next_token(parser) == 0 && 
                        parser->current_token.type == SQL_TOKEN_KEYWORD &&
                        sql_get_keyword(parser->current_token.value) == SQL_KW_KEY) {
                        column->primary_key = true;
                    }
                    break;
                case SQL_KW_UNIQUE:
                    column->unique = true;
                    break;
                case SQL_KW_NOT:
                    if (sql_parser_next_token(parser) == 0 && 
                        parser->current_token.type == SQL_TOKEN_KEYWORD &&
                        sql_get_keyword(parser->current_token.value) == SQL_KW_NULL) {
                        column->nullable = false;
                    }
                    break;
                default:
                    /* Ignore other keywords that are not column constraints */
                    break;
            }
        }
    }
    
    return 0;
}

/* Error handling */
void sql_parser_set_error(sql_parser_t *parser, const char *format, ...) {
    if (!parser) return;
    
    parser->has_error = true;
    va_list args;
    va_start(args, format);
    vsnprintf(parser->error_message, sizeof(parser->error_message), format, args);
    va_end(args);
}

bool sql_parser_has_error(sql_parser_t *parser) {
    return parser ? parser->has_error : false;
}

const char* sql_parser_get_error(sql_parser_t *parser) {
    return parser ? parser->error_message : NULL;
}

sql_token_t* sql_parser_get_current_token(sql_parser_t *parser) {
    return parser ? &parser->current_token : NULL;
}

/* Utility functions */
bool sql_parser_match_keyword(sql_parser_t *parser, const char *keyword) {
    if (!parser || parser->current_token.type != SQL_TOKEN_KEYWORD) {
        return false;
    }
    return strcasecmp(parser->current_token.value, keyword) == 0;
}

bool sql_parser_match_punctuation(sql_parser_t *parser, char punct) {
    if (!parser || parser->current_token.type != SQL_TOKEN_PUNCTUATION) {
        return false;
    }
    return parser->current_token.value[0] == punct;
}

bool sql_parser_expect_punctuation(sql_parser_t *parser, char punct) {
    if (!sql_parser_match_punctuation(parser, punct)) {
        sql_parser_set_error(parser, "Expected '%c'", punct);
        return false;
    }
    return true;
}

bool sql_parser_expect_keyword(sql_parser_t *parser, const char *keyword) {
    if (!sql_parser_match_keyword(parser, keyword)) {
        sql_parser_set_error(parser, "Expected '%s'", keyword);
        return false;
    }
    return true;
}

/* Statement cleanup */
void sql_statement_free(rdb_statement_t *stmt) {
    if (!stmt) return;
    
    if (stmt->columns) {
        for (size_t i = 0; i < fi_array_count(stmt->columns); i++) {
            rdb_column_t *col = *(rdb_column_t**)fi_array_get(stmt->columns, i);
            free(col);
        }
        fi_array_destroy(stmt->columns);
    }
    
    if (stmt->values) {
        for (size_t i = 0; i < fi_array_count(stmt->values); i++) {
            rdb_value_t *val = *(rdb_value_t**)fi_array_get(stmt->values, i);
            rdb_value_free(val);
        }
        fi_array_destroy(stmt->values);
    }
    
    if (stmt->where_conditions) {
        for (size_t i = 0; i < fi_array_count(stmt->where_conditions); i++) {
            sql_where_condition_t *cond = *(sql_where_condition_t**)fi_array_get(stmt->where_conditions, i);
            if (cond && cond->value) {
                rdb_value_free(cond->value);
            }
            free(cond);
        }
        fi_array_destroy(stmt->where_conditions);
    }
    
    if (stmt->select_columns) {
        fi_array_destroy(stmt->select_columns);
    }
    
    free(stmt);
}

/* Transaction parsing functions */
rdb_statement_t* sql_parse_begin_transaction(sql_parser_t *parser) {
    if (!parser) return NULL;
    
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    /* Initialize statement */
    stmt->type = RDB_STMT_BEGIN_TRANSACTION;
    stmt->table_name[0] = '\0';
    stmt->columns = NULL;
    stmt->values = NULL;
    stmt->where_conditions = NULL;
    stmt->select_columns = NULL;
    stmt->index_name[0] = '\0';
    stmt->index_column[0] = '\0';
    stmt->from_tables = NULL;
    stmt->join_conditions = NULL;
    stmt->order_by = NULL;
    stmt->limit_value = 0;
    stmt->offset_value = 0;
    stmt->foreign_key_name[0] = '\0';
    stmt->foreign_key = NULL;
    
    /* Parse optional TRANSACTION keyword */
    if (sql_parser_next_token(parser) == 0 && 
        parser->current_token.type == SQL_TOKEN_KEYWORD &&
        sql_get_keyword(parser->current_token.value) == SQL_KW_TRANSACTION) {
        sql_parser_next_token(parser);
    }
    
    return stmt;
}

rdb_statement_t* sql_parse_commit_transaction(sql_parser_t *parser) {
    if (!parser) return NULL;
    
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    /* Initialize statement */
    stmt->type = RDB_STMT_COMMIT_TRANSACTION;
    stmt->table_name[0] = '\0';
    stmt->columns = NULL;
    stmt->values = NULL;
    stmt->where_conditions = NULL;
    stmt->select_columns = NULL;
    stmt->index_name[0] = '\0';
    stmt->index_column[0] = '\0';
    stmt->from_tables = NULL;
    stmt->join_conditions = NULL;
    stmt->order_by = NULL;
    stmt->limit_value = 0;
    stmt->offset_value = 0;
    stmt->foreign_key_name[0] = '\0';
    stmt->foreign_key = NULL;
    
    /* Parse optional TRANSACTION keyword */
    if (sql_parser_next_token(parser) == 0 && 
        parser->current_token.type == SQL_TOKEN_KEYWORD &&
        sql_get_keyword(parser->current_token.value) == SQL_KW_TRANSACTION) {
        sql_parser_next_token(parser);
    }
    
    return stmt;
}

rdb_statement_t* sql_parse_rollback_transaction(sql_parser_t *parser) {
    if (!parser) return NULL;
    
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    /* Initialize statement */
    stmt->type = RDB_STMT_ROLLBACK_TRANSACTION;
    stmt->table_name[0] = '\0';
    stmt->columns = NULL;
    stmt->values = NULL;
    stmt->where_conditions = NULL;
    stmt->select_columns = NULL;
    stmt->index_name[0] = '\0';
    stmt->index_column[0] = '\0';
    stmt->from_tables = NULL;
    stmt->join_conditions = NULL;
    stmt->order_by = NULL;
    stmt->limit_value = 0;
    stmt->offset_value = 0;
    stmt->foreign_key_name[0] = '\0';
    stmt->foreign_key = NULL;
    
    /* Parse optional TRANSACTION keyword */
    if (sql_parser_next_token(parser) == 0 && 
        parser->current_token.type == SQL_TOKEN_KEYWORD &&
        sql_get_keyword(parser->current_token.value) == SQL_KW_TRANSACTION) {
        sql_parser_next_token(parser);
    }
    
    return stmt;
}

int sql_parse_isolation_level(sql_parser_t *parser, rdb_isolation_level_t *level) {
    if (!parser || !level) return -1;
    
    if (sql_parser_next_token(parser) != 0) return -1;
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
        sql_parser_set_error(parser, "Expected isolation level keyword");
        return -1;
    }
    
    sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
    
    switch (keyword) {
        case SQL_KW_READ:
            /* Check for READ UNCOMMITTED or READ COMMITTED */
            if (sql_parser_next_token(parser) != 0) return -1;
            if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
                sql_parser_set_error(parser, "Expected UNCOMMITTED or COMMITTED after READ");
                return -1;
            }
            
            sql_keyword_t read_keyword = sql_get_keyword(parser->current_token.value);
            if (read_keyword == SQL_KW_UNCOMMITTED) {
                *level = RDB_ISOLATION_READ_UNCOMMITTED;
            } else if (read_keyword == SQL_KW_COMMITTED) {
                *level = RDB_ISOLATION_READ_COMMITTED;
            } else {
                sql_parser_set_error(parser, "Expected UNCOMMITTED or COMMITTED after READ");
                return -1;
            }
            break;
            
        case SQL_KW_REPEATABLE:
            /* Check for REPEATABLE READ */
            if (sql_parser_next_token(parser) != 0) return -1;
            if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
                sql_get_keyword(parser->current_token.value) != SQL_KW_READ) {
                sql_parser_set_error(parser, "Expected READ after REPEATABLE");
                return -1;
            }
            *level = RDB_ISOLATION_REPEATABLE_READ;
            break;
            
        case SQL_KW_SERIALIZABLE:
            *level = RDB_ISOLATION_SERIALIZABLE;
            break;
            
        default:
            sql_parser_set_error(parser, "Invalid isolation level");
            return -1;
    }
    
    return 0;
}

/* Statement execution function */
int sql_execute_statement(rdb_database_t *db, const rdb_statement_t *stmt) {
    if (!db || !stmt) return -1;
    
    switch (stmt->type) {
        case RDB_STMT_BEGIN_TRANSACTION:
            return rdb_begin_transaction(db, RDB_ISOLATION_READ_COMMITTED);
            
        case RDB_STMT_COMMIT_TRANSACTION:
            return rdb_commit_transaction(db);
            
        case RDB_STMT_ROLLBACK_TRANSACTION:
            return rdb_rollback_transaction(db);
            
        default:
            printf("Error: Unsupported statement type for execution\n");
            return -1;
    }
}
