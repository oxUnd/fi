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
        "READ", "UNCOMMITTED", "COMMITTED", "REPEATABLE", "SERIALIZABLE",
        "TRUE", "FALSE", "LIKE", "IS", "IN"
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
        "READ", "UNCOMMITTED", "COMMITTED", "REPEATABLE", "SERIALIZABLE",
        "TRUE", "FALSE", "LIKE", "IS", "IN"
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
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_PUNCTUATION ||
        parser->current_token.value[0] != '(') {
        sql_parser_set_error(parser, "Expected '('");
        free(stmt);
        return NULL;
    }
    
    /* Advance past the opening parenthesis */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    /* Parse column definitions */
    stmt->columns = fi_array_create(16, sizeof(rdb_column_t*));
    if (!stmt->columns) {
        free(stmt);
        return NULL;
    }
    
    while (parser->current_token.type != SQL_TOKEN_PUNCTUATION || 
           parser->current_token.value[0] != ')') {
        
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
        /* Note: sql_parse_column_definition already advanced to the next token */
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION) {
            if (parser->current_token.value[0] == ')') {
                break;
            } else if (parser->current_token.value[0] == ',') {
                /* Advance past comma */
                if (sql_parser_next_token(parser) != 0) break;
            } else if (parser->current_token.value[0] == ';') {
                /* End of statement - this means we're done with columns */
                break;
            } else {
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

/* DROP statement parsing */
static rdb_statement_t* sql_parse_drop_statement(sql_parser_t *parser) {
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    /* Parse DROP TABLE or DROP INDEX */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
        sql_parser_set_error(parser, "Expected TABLE or INDEX after DROP");
        free(stmt);
        return NULL;
    }
    
    sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
    if (keyword == SQL_KW_TABLE) {
        stmt->type = RDB_STMT_DROP_TABLE;
        return sql_parse_drop_table(parser, stmt);
    } else if (keyword == SQL_KW_INDEX) {
        stmt->type = RDB_STMT_DROP_INDEX;
        return sql_parse_drop_index(parser, stmt);
    } else {
        sql_parser_set_error(parser, "Expected TABLE or INDEX after DROP");
        free(stmt);
        return NULL;
    }
}

rdb_statement_t* sql_parse_drop_table(sql_parser_t *parser, rdb_statement_t *stmt) {
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
    
    /* Initialize other fields */
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
    
    return stmt;
}

rdb_statement_t* sql_parse_drop_index(sql_parser_t *parser, rdb_statement_t *stmt) {
    /* Parse index name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected index name");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->index_name, parser->current_token.value, sizeof(stmt->index_name) - 1);
    stmt->index_name[sizeof(stmt->index_name) - 1] = '\0';
    
    /* Parse FROM table_name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_FROM) {
        sql_parser_set_error(parser, "Expected FROM after index name");
        free(stmt);
        return NULL;
    }
    
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name after FROM");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->table_name, parser->current_token.value, sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    
    /* Initialize other fields */
    stmt->columns = NULL;
    stmt->values = NULL;
    stmt->where_conditions = NULL;
    stmt->select_columns = NULL;
    stmt->index_column[0] = '\0';
    stmt->from_tables = NULL;
    stmt->join_conditions = NULL;
    stmt->order_by = NULL;
    stmt->limit_value = 0;
    stmt->offset_value = 0;
    stmt->foreign_key_name[0] = '\0';
    stmt->foreign_key = NULL;
    
    return stmt;
}

rdb_statement_t* sql_parse_insert(sql_parser_t *parser) {
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    stmt->type = RDB_STMT_INSERT;
    
    /* Parse INTO table_name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_INTO) {
        sql_parser_set_error(parser, "Expected INTO after INSERT");
        free(stmt);
        return NULL;
    }
    
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name after INTO");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->table_name, parser->current_token.value, sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    
    /* Parse column list (optional) */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type == SQL_TOKEN_PUNCTUATION && 
        parser->current_token.value[0] == '(') {
        /* Parse column list */
        stmt->columns = fi_array_create(16, sizeof(char*));
        if (!stmt->columns) {
            free(stmt);
            return NULL;
        }
        
        if (sql_parse_column_list(parser, stmt->columns) != 0) {
            fi_array_destroy(stmt->columns);
            free(stmt);
            return NULL;
        }
    } else {
        stmt->columns = NULL;
    }
    
    /* Parse VALUES keyword */
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_VALUES) {
        sql_parser_set_error(parser, "Expected VALUES");
        if (stmt->columns) fi_array_destroy(stmt->columns);
        free(stmt);
        return NULL;
    }
    
    /* Parse value list */
    stmt->values = fi_array_create(16, sizeof(rdb_value_t*));
    if (!stmt->values) {
        if (stmt->columns) fi_array_destroy(stmt->columns);
        free(stmt);
        return NULL;
    }
    
    if (sql_parse_value_list(parser, stmt->values) != 0) {
        if (stmt->columns) fi_array_destroy(stmt->columns);
        fi_array_destroy(stmt->values);
        free(stmt);
        return NULL;
    }
    
    /* Initialize other fields */
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
    
    return stmt;
}

rdb_statement_t* sql_parse_select(sql_parser_t *parser) {
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    stmt->type = RDB_STMT_SELECT;
    
    /* Parse column list */
    stmt->select_columns = fi_array_create(16, sizeof(char*));
    if (!stmt->select_columns) {
        free(stmt);
        return NULL;
    }
    
    if (sql_parser_next_token(parser) != 0) {
        fi_array_destroy(stmt->select_columns);
        free(stmt);
        return NULL;
    }
    
    /* Handle SELECT * */
    if (parser->current_token.type == SQL_TOKEN_PUNCTUATION && 
        parser->current_token.value[0] == '*') {
        char *star = malloc(2);
        if (!star) {
            fi_array_destroy(stmt->select_columns);
            free(stmt);
            return NULL;
        }
        strcpy(star, "*");
        fi_array_push(stmt->select_columns, &star);
    } else {
        /* Parse column list */
        if (sql_parse_column_list(parser, stmt->select_columns) != 0) {
            fi_array_destroy(stmt->select_columns);
            free(stmt);
            return NULL;
        }
    }
    
    /* Parse FROM clause */
    if (sql_parser_next_token(parser) != 0) {
        fi_array_destroy(stmt->select_columns);
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_FROM) {
        sql_parser_set_error(parser, "Expected FROM clause");
        fi_array_destroy(stmt->select_columns);
        free(stmt);
        return NULL;
    }
    
    /* Parse table list */
    stmt->from_tables = fi_array_create(16, sizeof(char*));
    if (!stmt->from_tables) {
        fi_array_destroy(stmt->select_columns);
        free(stmt);
        return NULL;
    }
    
    if (sql_parse_from_clause(parser, stmt->from_tables) != 0) {
        fi_array_destroy(stmt->select_columns);
        fi_array_destroy(stmt->from_tables);
        free(stmt);
        return NULL;
    }
    
    /* Parse optional WHERE clause */
    if (sql_parser_next_token(parser) == 0 &&
        parser->current_token.type == SQL_TOKEN_KEYWORD &&
        sql_get_keyword(parser->current_token.value) == SQL_KW_WHERE) {
        
        stmt->where_conditions = fi_array_create(16, sizeof(sql_where_condition_t*));
        if (!stmt->where_conditions) {
            fi_array_destroy(stmt->select_columns);
            fi_array_destroy(stmt->from_tables);
            free(stmt);
            return NULL;
        }
        
        if (sql_parse_where_clause(parser, stmt->where_conditions) != 0) {
            fi_array_destroy(stmt->select_columns);
            fi_array_destroy(stmt->from_tables);
            fi_array_destroy(stmt->where_conditions);
            free(stmt);
            return NULL;
        }
    } else {
        stmt->where_conditions = NULL;
    }
    
    /* Parse optional JOIN clauses */
    stmt->join_conditions = fi_array_create(16, sizeof(rdb_join_condition_t*));
    if (!stmt->join_conditions) {
        fi_array_destroy(stmt->select_columns);
        fi_array_destroy(stmt->from_tables);
        if (stmt->where_conditions) fi_array_destroy(stmt->where_conditions);
        free(stmt);
        return NULL;
    }
    
    /* Parse JOIN clauses if present */
    while (sql_parser_next_token(parser) == 0 &&
           parser->current_token.type == SQL_TOKEN_KEYWORD) {
        
        sql_keyword_t join_keyword = sql_get_keyword(parser->current_token.value);
        if (join_keyword == SQL_KW_JOIN || join_keyword == SQL_KW_INNER ||
            join_keyword == SQL_KW_LEFT || join_keyword == SQL_KW_RIGHT ||
            join_keyword == SQL_KW_FULL) {
            
            if (sql_parse_join_clause(parser, stmt->join_conditions) != 0) {
                fi_array_destroy(stmt->select_columns);
                fi_array_destroy(stmt->from_tables);
                if (stmt->where_conditions) fi_array_destroy(stmt->where_conditions);
                fi_array_destroy(stmt->join_conditions);
                free(stmt);
                return NULL;
            }
        } else {
            break;
        }
    }
    
    /* Initialize other fields */
    stmt->table_name[0] = '\0';
    stmt->columns = NULL;
    stmt->values = NULL;
    stmt->index_name[0] = '\0';
    stmt->index_column[0] = '\0';
    stmt->order_by = NULL;
    stmt->limit_value = 0;
    stmt->offset_value = 0;
    stmt->foreign_key_name[0] = '\0';
    stmt->foreign_key = NULL;
    
    return stmt;
}

rdb_statement_t* sql_parse_update(sql_parser_t *parser) {
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    stmt->type = RDB_STMT_UPDATE;
    
    /* Parse table name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name after UPDATE");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->table_name, parser->current_token.value, sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    
    /* Parse SET keyword */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_SET) {
        sql_parser_set_error(parser, "Expected SET after table name");
        free(stmt);
        return NULL;
    }
    
    /* Parse SET clause */
    stmt->columns = fi_array_create(16, sizeof(char*));
    stmt->values = fi_array_create(16, sizeof(rdb_value_t*));
    
    if (!stmt->columns || !stmt->values) {
        if (stmt->columns) fi_array_destroy(stmt->columns);
        if (stmt->values) fi_array_destroy(stmt->values);
        free(stmt);
        return NULL;
    }
    
    if (sql_parse_set_clause(parser, stmt->columns, stmt->values) != 0) {
        fi_array_destroy(stmt->columns);
        fi_array_destroy(stmt->values);
        free(stmt);
        return NULL;
    }
    
    /* Parse optional WHERE clause */
    if (sql_parser_next_token(parser) == 0 &&
        parser->current_token.type == SQL_TOKEN_KEYWORD &&
        sql_get_keyword(parser->current_token.value) == SQL_KW_WHERE) {
        
        stmt->where_conditions = fi_array_create(16, sizeof(sql_where_condition_t*));
        if (!stmt->where_conditions) {
            fi_array_destroy(stmt->columns);
            fi_array_destroy(stmt->values);
            free(stmt);
            return NULL;
        }
        
        if (sql_parse_where_clause(parser, stmt->where_conditions) != 0) {
            fi_array_destroy(stmt->columns);
            fi_array_destroy(stmt->values);
            fi_array_destroy(stmt->where_conditions);
            free(stmt);
            return NULL;
        }
    } else {
        stmt->where_conditions = NULL;
    }
    
    /* Initialize other fields */
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
    
    return stmt;
}

rdb_statement_t* sql_parse_delete(sql_parser_t *parser) {
    rdb_statement_t *stmt = malloc(sizeof(rdb_statement_t));
    if (!stmt) return NULL;
    
    stmt->type = RDB_STMT_DELETE;
    
    /* Parse FROM keyword */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_FROM) {
        sql_parser_set_error(parser, "Expected FROM after DELETE");
        free(stmt);
        return NULL;
    }
    
    /* Parse table name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name after FROM");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->table_name, parser->current_token.value, sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    
    /* Parse optional WHERE clause */
    if (sql_parser_next_token(parser) == 0 &&
        parser->current_token.type == SQL_TOKEN_KEYWORD &&
        sql_get_keyword(parser->current_token.value) == SQL_KW_WHERE) {
        
        stmt->where_conditions = fi_array_create(16, sizeof(sql_where_condition_t*));
        if (!stmt->where_conditions) {
            free(stmt);
            return NULL;
        }
        
        if (sql_parse_where_clause(parser, stmt->where_conditions) != 0) {
            fi_array_destroy(stmt->where_conditions);
            free(stmt);
            return NULL;
        }
    } else {
        stmt->where_conditions = NULL;
    }
    
    /* Initialize other fields */
    stmt->columns = NULL;
    stmt->values = NULL;
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
    
    return stmt;
}

static rdb_statement_t* sql_parse_create_index_simple(sql_parser_t *parser, rdb_statement_t *stmt) {
    /* Parse index name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected index name");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->index_name, parser->current_token.value, sizeof(stmt->index_name) - 1);
    stmt->index_name[sizeof(stmt->index_name) - 1] = '\0';
    
    /* Parse ON table_name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_ON) {
        sql_parser_set_error(parser, "Expected ON after index name");
        free(stmt);
        return NULL;
    }
    
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name after ON");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->table_name, parser->current_token.value, sizeof(stmt->table_name) - 1);
    stmt->table_name[sizeof(stmt->table_name) - 1] = '\0';
    
    /* Parse opening parenthesis */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_PUNCTUATION ||
        parser->current_token.value[0] != '(') {
        sql_parser_set_error(parser, "Expected opening parenthesis for column list");
        free(stmt);
        return NULL;
    }
    
    /* Parse column name */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected column name");
        free(stmt);
        return NULL;
    }
    
    strncpy(stmt->index_column, parser->current_token.value, sizeof(stmt->index_column) - 1);
    stmt->index_column[sizeof(stmt->index_column) - 1] = '\0';
    
    /* Parse closing parenthesis */
    if (sql_parser_next_token(parser) != 0) {
        free(stmt);
        return NULL;
    }
    
    if (parser->current_token.type != SQL_TOKEN_PUNCTUATION ||
        parser->current_token.value[0] != ')') {
        sql_parser_set_error(parser, "Expected closing parenthesis");
        free(stmt);
        return NULL;
    }
    
    /* Initialize other fields */
    stmt->columns = NULL;
    stmt->values = NULL;
    stmt->where_conditions = NULL;
    stmt->select_columns = NULL;
    stmt->from_tables = NULL;
    stmt->join_conditions = NULL;
    stmt->order_by = NULL;
    stmt->limit_value = 0;
    stmt->offset_value = 0;
    stmt->foreign_key_name[0] = '\0';
    stmt->foreign_key = NULL;
    
    return stmt;
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
            
            /* Check for length specification */
            if (sql_parser_next_token(parser) == 0 &&
                parser->current_token.type == SQL_TOKEN_PUNCTUATION &&
                parser->current_token.value[0] == '(') {
                
                /* Parse length */
                if (sql_parser_next_token(parser) != 0) {
                    sql_parser_set_error(parser, "Expected length for VARCHAR");
                    return -1;
                }
                
                if (parser->current_token.type != SQL_TOKEN_NUMBER) {
                    sql_parser_set_error(parser, "Expected number for VARCHAR length");
                    return -1;
                }
                
                column->max_length = atoi(parser->current_token.value);
                
                /* Parse closing parenthesis */
                if (sql_parser_next_token(parser) != 0) {
                    sql_parser_set_error(parser, "Expected closing parenthesis");
                    return -1;
                }
                
                if (parser->current_token.type != SQL_TOKEN_PUNCTUATION ||
                    parser->current_token.value[0] != ')') {
                    sql_parser_set_error(parser, "Expected closing parenthesis");
                    return -1;
                }
                
                /* Don't advance past the closing parenthesis - let the column parsing loop handle it */
            }
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
    sql_parser_next_token(parser); /* Advance to next token */
    return true;
}

bool sql_parser_expect_keyword(sql_parser_t *parser, const char *keyword) {
    if (!sql_parser_match_keyword(parser, keyword)) {
        sql_parser_set_error(parser, "Expected '%s'", keyword);
        return false;
    }
    sql_parser_next_token(parser); /* Advance to next token */
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

/* Column list parsing */
int sql_parse_column_list(sql_parser_t *parser, fi_array *columns) {
    /* Skip opening parenthesis */
    if (sql_parser_next_token(parser) != 0) return -1;
    
    while (true) {
        if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
            sql_parser_set_error(parser, "Expected column name");
            return -1;
        }
        
        char *column_name = malloc(strlen(parser->current_token.value) + 1);
        if (!column_name) return -1;
        strcpy(column_name, parser->current_token.value);
        
        fi_array_push(columns, &column_name);
        
        if (sql_parser_next_token(parser) != 0) return -1;
        
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION) {
            if (parser->current_token.value[0] == ')') {
                break;
            } else if (parser->current_token.value[0] != ',') {
                sql_parser_set_error(parser, "Expected comma or closing parenthesis");
                return -1;
            }
        }
        
        if (sql_parser_next_token(parser) != 0) return -1;
    }
    
    return 0;
}

/* Value list parsing */
int sql_parse_value_list(sql_parser_t *parser, fi_array *values) {
    /* Parse opening parenthesis */
    if (sql_parser_next_token(parser) != 0) return -1;
    
    if (parser->current_token.type != SQL_TOKEN_PUNCTUATION ||
        parser->current_token.value[0] != '(') {
        sql_parser_set_error(parser, "Expected opening parenthesis for VALUES");
        return -1;
    }
    
    while (true) {
        if (sql_parser_next_token(parser) != 0) return -1;
        
        rdb_value_t *value = sql_parse_value(parser);
        if (!value) return -1;
        
        fi_array_push(values, &value);
        
        if (sql_parser_next_token(parser) != 0) return -1;
        
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION) {
            if (parser->current_token.value[0] == ')') {
                break;
            } else if (parser->current_token.value[0] != ',') {
                sql_parser_set_error(parser, "Expected comma or closing parenthesis");
                return -1;
            }
        }
    }
    
    return 0;
}

/* Value parsing */
rdb_value_t* sql_parse_value(sql_parser_t *parser) {
    if (parser->current_token.type == SQL_TOKEN_STRING) {
        return sql_parse_string_value(parser->current_token.value);
    } else if (parser->current_token.type == SQL_TOKEN_NUMBER) {
        return sql_parse_number_value(parser->current_token.value);
    } else if (parser->current_token.type == SQL_TOKEN_KEYWORD) {
        sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
        if (keyword == SQL_KW_NULL) {
            return rdb_create_null_value(RDB_TYPE_INT); /* Default type for NULL */
        } else if (keyword == SQL_KW_TRUE || keyword == SQL_KW_FALSE) {
            return sql_parse_boolean_value(parser->current_token.value);
        }
    }
    
    sql_parser_set_error(parser, "Invalid value");
    return NULL;
}

rdb_value_t* sql_parse_string_value(const char *str) {
    return rdb_create_string_value(str);
}

rdb_value_t* sql_parse_number_value(const char *str) {
    char *endptr;
    double num = strtod(str, &endptr);
    
    if (*endptr == '\0') {
        /* Check if it's an integer */
        if (strchr(str, '.') == NULL && strchr(str, 'e') == NULL && strchr(str, 'E') == NULL) {
            return rdb_create_int_value((int64_t)num);
        } else {
            return rdb_create_float_value(num);
        }
    }
    
    return NULL;
}

rdb_value_t* sql_parse_boolean_value(const char *str) {
    if (strcasecmp(str, "TRUE") == 0) {
        return rdb_create_bool_value(true);
    } else if (strcasecmp(str, "FALSE") == 0) {
        return rdb_create_bool_value(false);
    }
    return NULL;
}

/* FROM clause parsing */
int sql_parse_from_clause(sql_parser_t *parser, fi_array *from_tables) {
    if (sql_parser_next_token(parser) != 0) return -1;
    
    while (true) {
        if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
            sql_parser_set_error(parser, "Expected table name in FROM clause");
            return -1;
        }
        
        char *table_name = malloc(strlen(parser->current_token.value) + 1);
        if (!table_name) return -1;
        strcpy(table_name, parser->current_token.value);
        
        fi_array_push(from_tables, &table_name);
        
        if (sql_parser_next_token(parser) != 0) return -1;
        
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION) {
            if (parser->current_token.value[0] == ',') {
                if (sql_parser_next_token(parser) != 0) return -1;
                continue;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    
    return 0;
}

/* WHERE clause parsing */
int sql_parse_where_clause(sql_parser_t *parser, fi_array *conditions) {
    if (sql_parser_next_token(parser) != 0) return -1;
    
    while (true) {
        sql_where_condition_t *condition = malloc(sizeof(sql_where_condition_t));
        if (!condition) return -1;
        
        /* Parse column name */
        if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
            sql_parser_set_error(parser, "Expected column name in WHERE clause");
            free(condition);
            return -1;
        }
        
        strncpy(condition->column_name, parser->current_token.value, sizeof(condition->column_name) - 1);
        condition->column_name[sizeof(condition->column_name) - 1] = '\0';
        
        /* Parse operator */
        if (sql_parser_next_token(parser) != 0) {
            free(condition);
            return -1;
        }
        
        if (parser->current_token.type == SQL_TOKEN_OPERATOR) {
            condition->operator = sql_get_operator(parser->current_token.value);
        } else if (parser->current_token.type == SQL_TOKEN_KEYWORD) {
            sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
            if (keyword == SQL_KW_LIKE) {
                condition->operator = SQL_OP_LIKE;
            } else if (keyword == SQL_KW_IS) {
                condition->operator = SQL_OP_IS;
            } else if (keyword == SQL_KW_IN) {
                condition->operator = SQL_OP_IN;
            } else {
                sql_parser_set_error(parser, "Invalid operator in WHERE clause");
                free(condition);
                return -1;
            }
        } else {
            sql_parser_set_error(parser, "Expected operator in WHERE clause");
            free(condition);
            return -1;
        }
        
        /* Parse value */
        if (sql_parser_next_token(parser) != 0) {
            free(condition);
            return -1;
        }
        
        condition->value = sql_parse_value(parser);
        if (!condition->value) {
            free(condition);
            return -1;
        }
        
        /* Initialize logical connector */
        condition->logical_connector[0] = '\0';
        
        fi_array_push(conditions, &condition);
        
        if (sql_parser_next_token(parser) != 0) return -1;
        
        /* Check for logical operators */
        if (parser->current_token.type == SQL_TOKEN_KEYWORD) {
            sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
            if (keyword == SQL_KW_AND || keyword == SQL_KW_OR) {
                strncpy(condition->logical_connector, parser->current_token.value, sizeof(condition->logical_connector) - 1);
                condition->logical_connector[sizeof(condition->logical_connector) - 1] = '\0';
                
                if (sql_parser_next_token(parser) != 0) return -1;
                continue;
            }
        }
        
        break;
    }
    
    return 0;
}

/* JOIN clause parsing */
int sql_parse_join_clause(sql_parser_t *parser, fi_array *join_conditions) {
    rdb_join_condition_t *condition = malloc(sizeof(rdb_join_condition_t));
    if (!condition) return -1;
    
    /* Parse join type */
    if (sql_parse_join_type(parser, &condition->join_type) != 0) {
        free(condition);
        return -1;
    }
    
    /* Parse table name */
    if (sql_parser_next_token(parser) != 0) {
        free(condition);
        return -1;
    }
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table name in JOIN clause");
        free(condition);
        return -1;
    }
    
    strncpy(condition->right_table, parser->current_token.value, sizeof(condition->right_table) - 1);
    condition->right_table[sizeof(condition->right_table) - 1] = '\0';
    
    /* Parse ON condition */
    if (sql_parser_next_token(parser) != 0) {
        free(condition);
        return -1;
    }
    
    if (parser->current_token.type != SQL_TOKEN_KEYWORD ||
        sql_get_keyword(parser->current_token.value) != SQL_KW_ON) {
        sql_parser_set_error(parser, "Expected ON in JOIN clause");
        free(condition);
        return -1;
    }
    
    if (sql_parse_join_condition(parser, condition) != 0) {
        free(condition);
        return -1;
    }
    
    fi_array_push(join_conditions, &condition);
    return 0;
}

int sql_parse_join_type(sql_parser_t *parser, rdb_join_type_t *join_type) {
    if (parser->current_token.type != SQL_TOKEN_KEYWORD) {
        sql_parser_set_error(parser, "Expected JOIN type");
        return -1;
    }
    
    sql_keyword_t keyword = sql_get_keyword(parser->current_token.value);
    switch (keyword) {
        case SQL_KW_JOIN:
        case SQL_KW_INNER:
            *join_type = RDB_JOIN_INNER;
            break;
        case SQL_KW_LEFT:
            *join_type = RDB_JOIN_LEFT;
            break;
        case SQL_KW_RIGHT:
            *join_type = RDB_JOIN_RIGHT;
            break;
        case SQL_KW_FULL:
            *join_type = RDB_JOIN_FULL;
            break;
        default:
            sql_parser_set_error(parser, "Invalid JOIN type");
            return -1;
    }
    
    return 0;
}

int sql_parse_join_condition(sql_parser_t *parser, rdb_join_condition_t *condition) {
    /* Parse left table.column */
    if (sql_parser_next_token(parser) != 0) return -1;
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table.column in JOIN condition");
        return -1;
    }
    
    char *left_ref = parser->current_token.value;
    char *dot = strchr(left_ref, '.');
    if (dot) {
        size_t table_len = dot - left_ref;
        strncpy(condition->left_table, left_ref, table_len);
        condition->left_table[table_len] = '\0';
        strncpy(condition->left_column, dot + 1, sizeof(condition->left_column) - 1);
        condition->left_column[sizeof(condition->left_column) - 1] = '\0';
    } else {
        sql_parser_set_error(parser, "Expected table.column format in JOIN condition");
        return -1;
    }
    
    /* Parse = operator */
    if (sql_parser_next_token(parser) != 0) return -1;
    
    if (parser->current_token.type != SQL_TOKEN_OPERATOR ||
        parser->current_token.value[0] != '=') {
        sql_parser_set_error(parser, "Expected = in JOIN condition");
        return -1;
    }
    
    /* Parse right table.column */
    if (sql_parser_next_token(parser) != 0) return -1;
    
    if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
        sql_parser_set_error(parser, "Expected table.column in JOIN condition");
        return -1;
    }
    
    char *right_ref = parser->current_token.value;
    dot = strchr(right_ref, '.');
    if (dot) {
        size_t table_len = dot - right_ref;
        strncpy(condition->right_table, right_ref, table_len);
        condition->right_table[table_len] = '\0';
        strncpy(condition->right_column, dot + 1, sizeof(condition->right_column) - 1);
        condition->right_column[sizeof(condition->right_column) - 1] = '\0';
    } else {
        sql_parser_set_error(parser, "Expected table.column format in JOIN condition");
        return -1;
    }
    
    return 0;
}

/* SET clause parsing */
int sql_parse_set_clause(sql_parser_t *parser, fi_array *columns, fi_array *values) {
    while (true) {
        /* Parse column name */
        if (sql_parser_next_token(parser) != 0) return -1;
        
        if (parser->current_token.type != SQL_TOKEN_IDENTIFIER) {
            sql_parser_set_error(parser, "Expected column name in SET clause");
            return -1;
        }
        
        char *column_name = malloc(strlen(parser->current_token.value) + 1);
        if (!column_name) return -1;
        strcpy(column_name, parser->current_token.value);
        
        fi_array_push(columns, &column_name);
        
        /* Parse = operator */
        if (sql_parser_next_token(parser) != 0) return -1;
        
        if (parser->current_token.type != SQL_TOKEN_OPERATOR ||
            parser->current_token.value[0] != '=') {
            sql_parser_set_error(parser, "Expected = in SET clause");
            return -1;
        }
        
        /* Parse value */
        if (sql_parser_next_token(parser) != 0) return -1;
        
        rdb_value_t *value = sql_parse_value(parser);
        if (!value) return -1;
        
        fi_array_push(values, &value);
        
        /* Check for comma */
        if (sql_parser_next_token(parser) != 0) return -1;
        
        if (parser->current_token.type == SQL_TOKEN_PUNCTUATION &&
            parser->current_token.value[0] == ',') {
            continue;
        } else {
            break;
        }
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
