#ifndef __SQL_PARSER_H__
#define __SQL_PARSER_H__

#include "rdb.h"

/* SQL token types */
typedef enum {
    SQL_TOKEN_KEYWORD,
    SQL_TOKEN_IDENTIFIER,
    SQL_TOKEN_STRING,
    SQL_TOKEN_NUMBER,
    SQL_TOKEN_OPERATOR,
    SQL_TOKEN_PUNCTUATION,
    SQL_TOKEN_EOF,
    SQL_TOKEN_UNKNOWN
} sql_token_type_t;

/* SQL token structure */
typedef struct {
    sql_token_type_t type;
    char *value;
    size_t length;
    size_t position;
} sql_token_t;

/* SQL parser state */
typedef struct {
    char *sql;
    size_t pos;
    size_t length;
    sql_token_t current_token;
    bool has_error;
    char error_message[256];
} sql_parser_t;

/* SQL keywords */
typedef enum {
    SQL_KW_SELECT = 1,
    SQL_KW_FROM,
    SQL_KW_WHERE,
    SQL_KW_INSERT,
    SQL_KW_INTO,
    SQL_KW_VALUES,
    SQL_KW_UPDATE,
    SQL_KW_SET,
    SQL_KW_DELETE,
    SQL_KW_CREATE,
    SQL_KW_TABLE,
    SQL_KW_INDEX,
    SQL_KW_DROP,
    SQL_KW_INT,
    SQL_KW_FLOAT,
    SQL_KW_VARCHAR,
    SQL_KW_TEXT,
    SQL_KW_BOOLEAN,
    SQL_KW_PRIMARY,
    SQL_KW_KEY,
    SQL_KW_UNIQUE,
    SQL_KW_NOT,
    SQL_KW_NULL,
    SQL_KW_DEFAULT,
    SQL_KW_AND,
    SQL_KW_OR,
    SQL_KW_ORDER,
    SQL_KW_BY,
    SQL_KW_LIMIT,
    SQL_KW_OFFSET
} sql_keyword_t;

/* SQL operators */
typedef enum {
    SQL_OP_EQUAL = 1,
    SQL_OP_NOT_EQUAL,
    SQL_OP_LESS_THAN,
    SQL_OP_GREATER_THAN,
    SQL_OP_LESS_EQUAL,
    SQL_OP_GREATER_EQUAL,
    SQL_OP_LIKE,
    SQL_OP_IS,
    SQL_OP_IN
} sql_operator_t;

/* WHERE condition */
typedef struct {
    char column_name[64];
    sql_operator_t operator;
    rdb_value_t *value;
    char logical_connector[8]; /* AND, OR */
} sql_where_condition_t;

/* Parser functions */
sql_parser_t* sql_parser_create(const char *sql);
void sql_parser_destroy(sql_parser_t *parser);

/* Tokenization */
int sql_parser_next_token(sql_parser_t *parser);
sql_token_t* sql_parser_get_current_token(sql_parser_t *parser);
bool sql_parser_has_error(sql_parser_t *parser);
const char* sql_parser_get_error(sql_parser_t *parser);

/* Statement parsing */
rdb_statement_t* sql_parse_statement(sql_parser_t *parser);
rdb_statement_t* sql_parse_create_table(sql_parser_t *parser, rdb_statement_t *stmt);
rdb_statement_t* sql_parse_drop_table(sql_parser_t *parser);
rdb_statement_t* sql_parse_insert(sql_parser_t *parser);
rdb_statement_t* sql_parse_select(sql_parser_t *parser);
rdb_statement_t* sql_parse_update(sql_parser_t *parser);
rdb_statement_t* sql_parse_delete(sql_parser_t *parser);
rdb_statement_t* sql_parse_create_index(sql_parser_t *parser);
rdb_statement_t* sql_parse_drop_index(sql_parser_t *parser);

/* Helper functions */
sql_keyword_t sql_get_keyword(const char *keyword);
bool sql_is_keyword(const char *word);
bool sql_is_operator(const char *op);
sql_operator_t sql_get_operator(const char *op);
bool sql_is_punctuation(char c);
bool sql_is_whitespace(char c);
bool sql_is_identifier_char(char c);

/* Column parsing */
int sql_parse_column_definition(sql_parser_t *parser, rdb_column_t *column);
int sql_parse_column_list(sql_parser_t *parser, fi_array *columns);
int sql_parse_value_list(sql_parser_t *parser, fi_array *values);
int sql_parse_where_clause(sql_parser_t *parser, fi_array *conditions);

/* Value parsing */
rdb_value_t* sql_parse_value(sql_parser_t *parser);
rdb_value_t* sql_parse_string_value(const char *str);
rdb_value_t* sql_parse_number_value(const char *str);
rdb_value_t* sql_parse_boolean_value(const char *str);

/* Statement execution */
int sql_execute_statement(rdb_database_t *db, const rdb_statement_t *stmt);

/* Utility functions */
void sql_parser_skip_whitespace(sql_parser_t *parser);
char sql_parser_peek_char(sql_parser_t *parser);
char sql_parser_next_char(sql_parser_t *parser);
bool sql_parser_match_keyword(sql_parser_t *parser, const char *keyword);
bool sql_parser_match_punctuation(sql_parser_t *parser, char punct);
bool sql_parser_expect_punctuation(sql_parser_t *parser, char punct);
bool sql_parser_expect_keyword(sql_parser_t *parser, const char *keyword);

/* Error handling */
void sql_parser_set_error(sql_parser_t *parser, const char *format, ...);

/* Statement cleanup */
void sql_statement_free(rdb_statement_t *stmt);
void sql_where_condition_free(void *condition);

#endif //__SQL_PARSER_H__
