/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar TrinityLakeSqlExtensions;

singleStatement
    : statement EOF
    ;

statement
    : beginStatement
    | commitStatement
    | rollbackStatement
    ;

beginStatement
    : BEGIN (TRANSACTION)?
    ;

commitStatement
    : COMMIT (TRANSACTION)?
    ;

rollbackStatement
    : ROLLBACK (TRANSACTION)?
    ;

nonReserved
    : BEGIN | COMMIT | ROLLBACK | TRANSACTION
    ;

BEGIN: 'BEGIN';
TRANSACTION: 'TRANSACTION';
COMMIT: 'COMMIT';
ROLLBACK: 'ROLLBACK';

SIMPLE_COMMENT
    : '--' (~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we don't recognize
UNRECOGNIZED
    : .
    ;
