#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

// 输入缓冲区结构体
typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

// 执行结果枚举
typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
} ExecuteResult;

// 元命令结果枚举
typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// 准备语句结果枚举
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// 语句类型枚举
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

// 行定义
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

// 语句结构体
typedef struct {
  StatementType type; 
  Row row_to_insert;  // 要插入的行,仅用于insert语句
} Statement;

/* 
 *    表常量定义，不创建实际变量就能获取结构体中某个成员的大小
 *    size_of_attribute(Row, name)，展开后
 *    sizeof(((Row*)0)->name)
 */
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

//以4KB为单位的分页存储
const uint32_t PAGE_SIZE = 4096;
//一个表最多可以占用 400 个页面
#define TABLE_MAX_PAGES 400
//特殊标记，用于表示"页号无效"
#define INVALID_PAGE_NUM UINT32_MAX

// 页管理器
typedef struct {
  int file_descriptor;// 操作系统分配文件描述符
  uint32_t file_length;
  uint32_t num_pages;
  void* pages[TABLE_MAX_PAGES];
} Pager;

// 表结构体
typedef struct {
  Pager* pager;
  uint32_t root_page_num;// 表的根节点在第几页
} Table;

/*
数据库目录/
   │
   ├── users.db (文件)
   │      │
   │      └── Pager A
   │            ├── fd = 3
   │            ├── file_length = 12288
   │            ├── num_pages = 3
   │            └── pages[0..2]
   │
   └── orders.db (文件)
          │
          └── Pager B
                ├── fd = 4
                ├── file_length = 8192
                ├── num_pages = 2
                └── pages[0..1]
*/

// 光标结构体
typedef struct {
    Table* table;        // 指向哪个表
    uint32_t page_num;   // 当前页号
    uint32_t cell_num;   // 当前单元格号
    bool end_of_table;   // 是否到达表的末尾
} Cursor;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// 节点类型枚举
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

/* 通用节点头部布局
   通用节点头部 (Common Node Header) - 总共 6 字节

字节序号:0      1      2             3             4             5
        ↓      ↓      ↓             ↓             ↓             ↓
        ┌──────┬──────┬─────────────────────────────────────────┐
        │Type  │ Root │           Parent Pointer                │
        │(1B)  │(1B)  │              (4 Bytes)                  │
        └──────┴──────┴─────────────────────────────────────────┘
        ↑      ↑      ↑                                         ↑
        Offset Offset Offset                                    Offset
        0      1      2                                         5

字段说明:
┌─────────────────┬──────────┬────────┬────────────────────────────┐
│ 字段名称         │ 偏移量   │ 大小   │     说明                    │
├─────────────────┼──────────┼────────┼────────────────────────────┤
│ NODE_TYPE       │ 0        │ 1 字节  │ 节点类型(0=内部,1=叶子)     │
│ IS_ROOT         │ 1        │ 1 字节 │ 是否根节点(0=否,1=是)        │
│ PARENT_POINTER  │ 2        │ 4 字节 │ 父节点的页号                 │
└─────────────────┴──────────┴────────┴────────────────────────────┘
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/* 内部节点头部布局
内部节点头部 (Internal Node Header) - 总共 14 字节
┌─────────────────────────────────────────────────────────────────────────────┐
│                          COMMON NODE HEADER (6 bytes)                       │
├───────────┬───────────┬─────────────────────────────────────────────────────┤
│ Node Type │ Is Root   │                 Parent Pointer                      │
│ (1 byte)  │ (1 byte)  │                    (4 bytes)                        │
├───────────┴───────────┴─────────────────────────────────────────────────────┤
│                       INTERNAL-SPECIFIC HEADER (8 bytes)                    │
├───────────────────────────────────────────────────────┬─────────────────────┤
│                 Num Keys                              │     Right Child     │
│                 (4 bytes)                             │     (4 bytes)       │
├───────────────────────────────────────────────────────┼─────────────────────┤
│ Offset: 6                                             │ Offset: 10          │
└───────────────────────────────────────────────────────┴─────────────────────┘
总大小: 6 + 4 + 4 = 14 字节 (和叶子节点头部大小相同)
*/
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/* 内部节点主体布局
内部节点单个单元格 (Cell) 的结构:
┌──────────────────────┬──────────────────────┐
│         KEY          │        CHILD         │
│   (4 bytes)          │     (4 bytes)        │
├──────────────────────┼──────────────────────┤
│ Offset: 0            │ Offset: 4            │
└──────────────────────┴──────────────────────┘
INTERNAL_NODE_CELL_SIZE = 4 + 4 = 8 字节
*/ 
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
/* Keep this small for testing */
//const uint32_t INTERNAL_NODE_MAX_KEYS = 3;

const uint32_t INTERNAL_NODE_MAX_KEYS = (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE) / INTERNAL_NODE_CELL_SIZE;

/* 叶子节点头部布局
叶子节点头部 (Leaf Node Header) - 总共 14 字节

┌─────────────────────────────────────────────────────────────────────────────┐
│                          COMMON NODE HEADER (6 bytes)                       │
├───────────┬───────────┬─────────────────────────────────────────────────────┤
│ Node Type │ Is Root   │                 Parent Pointer                      │
│ (1 byte)  │ (1 byte)  │                    (4 bytes)                        │
├───────────┴───────────┴─────────────────────────────────────────────────────┤
│                         LEAF_NODE_SPECFIC_SIZE (8 bytes)                    │
├───────────────────────────────────────────────────────┬─────────────────────┤
│                 Num Cells                             │     Next Leaf       │
│                 (4 bytes)                             │     (4 bytes)       │
├───────────────────────────────────────────────────────┼─────────────────────┤
│ Offset:6                                              │ Offset:10           │
└───────────────────────────────────────────────────────┴─────────────────────┘
总大小: 6 + 4 + 4 = 14 字节
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

/* 叶子节点主体布局
单个单元格 (Cell) 的结构:
┌──────────────────────┬──────────────────────────────────┐
│       KEY            │              VALUE               │
│   (4 bytes)          │         (ROW_SIZE bytes)         │
├──────────────────────┼──────────────────────────────────┤
│ Offset: 0            │ Offset: 4                        │
└──────────────────────┴──────────────────────────────────┘
LEAF_NODE_CELL_SIZE = 4 + ROW_SIZE
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;


/*
Table
  ├── root_page_num = 0
  └── Pager (页管理器)
        ├── file_descriptor = 3
        ├── file_length = 16384
        ├── num_pages = 4
        └── pages[0] ──→ 0x1000 (内存地址) ──→ 这是第0页，也是节点0
            pages[1] ──→ 0x2000 (内存地址) ──→ 这是第1页，也是节点1
            pages[2] ──→ 0x3000 (内存地址) ──→ 这是第2页，也是节点2
            pages[3] ──→ 0x4000 (内存地址) ──→ 这是第3页，也是节点3
*/

/* 层次结构
数据库 = 一个文件
   │
   └─── 表 = 文件里的数据集合
          │
          └─── 页 = 文件被分成 4KB 的小块
                 │
                 └─── 节点 = 每个页里放一个节点
                        │
                        ├─── 叶子节点 = 真正存数据的节点
                        │      │
                        │      └─── 单元格 = 每个单元格存一行数据
                        │
                        └─── 内部节点 = 只存索引的节点（指向其他节点）
*/

/*
┌─────────────────────────────────────────────────────────────────────────────┐
│                           B+TREE 索引结构                                    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        根节点 (内部节点)                             │    │
│  │  ┌─────────┬─────────┬─────────┬─────────────────────────────┐      │    │
│  │  │  Key:50 │ Child:1 │ Key:100 │ Child:2 │ Right Child:3     │      │    │
│  │  │ "小于50"│          │ "50-100"│        │   "大于100"        │      │   │
│  │  └─────────┴─────────┴─────────┴─────────┴───────────────────┘      │   │
│  └───────────────────────────┬─────────────────┬───────────────────────┘   │
│                              │                 │                           │
│          ┌───────────────────┴──────┐          └───────────┐               │
│          ▼                          ▼                      ▼               │
│  ┌──────────────────┐      ┌──────────────────┐   ┌──────────────────┐     │
│  │  内部节点(页1)    │      │  内部节点(页2)    │   │  内部节点(页3)    │     │
│  │  Key:20→页4      │      │  Key:75→页7      │   │  Key:150→页10    │     │
│  │  Key:35→页5      │      │  Key:90→页8      │   │  Key:200→页11    │     │
│  │  Right:页6       │      │  Right:页9       │   │  Right:页12      │     │
│  └────────┬─────────┘      └────────┬─────────┘   └────────┬─────────┘     │
│           │                         │                      │               │
│      ┌────┴────┐                ┌───┴───┐              ┌───┴───┐           │
│      ▼    ▼    ▼                ▼   ▼   ▼              ▼   ▼   ▼           │
│    ┌────┐...  ┌────┐          ┌────┐...            ┌────┐                  │
│    │叶子│     │叶子│          │叶子│                │叶子│                   │
│    │节点│     │节点│          │节点│                │节点│                   │
│    │页4 │     │页6 │         │页7 │                │页10│                   │
│    │    │    │    │          │    │                │    │                   │
│    │存  │    │存  │          │存  │                │存  │                   │
│    │行  │    │行  │          │行  │                │行  │                   │
│    │数  │    │数  │          │数  │                │数  │                    │
│    │据  │    │据  │          │据  │                │据  │                    │
│    └────┘    └────┘          └────┘                └────┘                   │
│                                                                             │
│  内部节点作用: 索引/路标    │  叶子节点作用: 存储真实数据                       │
└─────────────────────────────────────────────────────────────────────────────┘
叶子节点通过 Next Leaf 指针连接成单向链表，用于范围扫描。
叶子节点链表（单向） example:key 50 ~ 100
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Page 0    │    │   Page 1    │    │   Page 2    │
│ 叶子节点     │───→│ 叶子节点     │───→│ 叶子节点    │
│ Next Leaf=1 │    │ Next Leaf=2 │    │ Next Leaf=0 │
│             │    │             │    │ (INVALID)   │
└─────────────┘    └─────────────┘    └─────────────┘
*/

// 获取节点类型
NodeType get_node_type(void* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

// 设置节点类型
void set_node_type(void* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

// 判断节点是否为根
bool is_node_root(void* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

// 设置节点是否为根
void set_node_root(void* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

// 获取节点父指针
uint32_t* node_parent(void* node) { return node + PARENT_POINTER_OFFSET; }

// 内部节点访问函数
uint32_t* internal_node_num_keys(void* node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    uint32_t* right_child = internal_node_right_child(node);
    if (*right_child == INVALID_PAGE_NUM) {
      printf("Tried to access right child of node, but was invalid page\n");
      exit(EXIT_FAILURE);
    }
    return right_child;
  } else {
    uint32_t* child = internal_node_cell(node, child_num);
    if (*child == INVALID_PAGE_NUM) {
      printf("Tried to access child %d of node, but was invalid page\n", child_num);
      exit(EXIT_FAILURE);
    }
    return child;
  }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
  return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

// 叶子节点访问函数
uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// 获取页面
void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss. 分配内存并加载
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

   
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num < num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

// 获取节点最大键
uint32_t get_node_max_key(Pager* pager, void* node) {
  if (get_node_type(node) == NODE_LEAF) {
    return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
  void* right_child = get_page(pager,*internal_node_right_child(node));
  return get_node_max_key(pager, right_child);
}

//打印常量
void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

// 缩进打印
void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

// 打印B树结构
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
  void* node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_node_key(node, i));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      if (num_keys > 0) {
        for (uint32_t i = 0; i < num_keys; i++) {
          child = *internal_node_child(node, i);
          print_tree(pager, child, indentation_level + 1);

          indent(indentation_level + 1);
          printf("- key %d\n", *internal_node_key(node, i));
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
      }
      break;
  }
}

// 序列化行到内存
void serialize_row(Row* source, void* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// 从内存反序列化行
void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}


void initialize_leaf_node(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;  
}

void initialize_internal_node(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;

  *internal_node_right_child(node) = INVALID_PAGE_NUM;
}

// 在叶子节点中查找键
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;
  cursor->end_of_table = false;

  
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;
  return cursor;
}

// 内部节点辅助函数
uint32_t internal_node_find_child(void* node, uint32_t key) {


  uint32_t num_keys = *internal_node_num_keys(node);

  // 二分查找范围是 [0, num_keys]
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; 

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

// 内部节点查找
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);

  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void* child = get_page(table->pager, child_num);
  switch (get_node_type(child)) {
    case NODE_LEAF:
      return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
      return internal_node_find(table, child_num, key);
  }
}

// 查找键在表中的位置 - 支持内部节点
Cursor* table_find(Table* table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
  }
}

// 创建指向表开始的光标 - 找到最左边的叶子节点
Cursor* table_start(Table* table) {
  Cursor* cursor = table_find(table, 0);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

// 获取光标位置的值
void* cursor_value(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
}

// 前进光标 - 支持叶子节点链表遍历
void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
   
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

// 打开分页器
Pager* pager_open(const char* filename) {
  int fd = open(filename,
                O_RDWR |      // Read/Write mode
                O_CREAT,      // Create file if it does not exist
                S_IWUSR |     // User write permission
                S_IRUSR       // User read permission
                );

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

// 打开数据库
Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  if (pager->num_pages == 0) {

    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  return table;
}

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }


  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

// 刷新页面到磁盘
void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

void db_close(Table* table) {
  Pager* pager = table->pager;

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

// 处理元命令
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_tree(table->pager, 0, 0);
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

// 准备插入语句
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_INSERT;

  char* keyword = strtok(input_buffer->buffer, " ");
  char* id_string = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

// 准备语句
PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

// 获取未使用的页码
uint32_t get_unused_page_num(Pager* pager) { return pager->num_pages; }

// 创建新根节点
void create_new_root(Table* table, uint32_t right_child_page_num) {
 
  void* root = get_page(table->pager, table->root_page_num);
  void* right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void* left_child = get_page(table->pager, left_child_page_num);

  if (get_node_type(root) == NODE_INTERNAL) {
    initialize_internal_node(right_child);
    initialize_internal_node(left_child);
  }


  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  if (get_node_type(left_child) == NODE_INTERNAL) {
    void* child;
    for (int i = 0; i < *internal_node_num_keys(left_child); i++) {
      child = get_page(table->pager, *internal_node_child(left_child,i));
      *node_parent(child) = left_child_page_num;
    }
    child = get_page(table->pager, *internal_node_right_child(left_child));
    *node_parent(child) = left_child_page_num;
  }


  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num);

// 内部节点插入数据
void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {


  void* parent = get_page(table->pager, parent_page_num);
  void* child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(table->pager, child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);

  if (original_num_keys >= INTERNAL_NODE_MAX_KEYS) {
    internal_node_split_and_insert(table, parent_page_num, child_page_num);
    return;
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);

  if (right_child_page_num == INVALID_PAGE_NUM) {
    *internal_node_right_child(parent) = child_page_num;
    return;
  }

  void* right_child = get_page(table->pager, right_child_page_num);

  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (child_max_key > get_node_max_key(table->pager, right_child)) {
  
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(table->pager, right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    
    for (uint32_t i = original_num_keys; i > index; i--) {
      void* destination = internal_node_cell(parent, i);
      void* source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}

// 更新内部节点key
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

// 内部节点分裂并插入
void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  uint32_t old_page_num = parent_page_num;
  void* old_node = get_page(table->pager,parent_page_num);
  uint32_t old_max = get_node_max_key(table->pager, old_node);

  void* child = get_page(table->pager, child_page_num); 
  uint32_t child_max = get_node_max_key(table->pager, child);

  uint32_t new_page_num = get_unused_page_num(table->pager);


  uint32_t splitting_root = is_node_root(old_node);

  void* parent;
  void* new_node;
  if (splitting_root) {
    create_new_root(table, new_page_num);
    parent = get_page(table->pager,table->root_page_num);

    old_page_num = *internal_node_child(parent,0);
    old_node = get_page(table->pager, old_page_num);
  } else {
    parent = get_page(table->pager,*node_parent(old_node));
    new_node = get_page(table->pager, new_page_num);
    initialize_internal_node(new_node);
  }
  
  uint32_t* old_num_keys = internal_node_num_keys(old_node);

  uint32_t cur_page_num = *internal_node_right_child(old_node);
  void* cur = get_page(table->pager, cur_page_num);


  internal_node_insert(table, new_page_num, cur_page_num);
  *node_parent(cur) = new_page_num;
  *internal_node_right_child(old_node) = INVALID_PAGE_NUM;
 
  for (int i = INTERNAL_NODE_MAX_KEYS - 1; i > INTERNAL_NODE_MAX_KEYS / 2; i--) {
    cur_page_num = *internal_node_child(old_node, i);
    cur = get_page(table->pager, cur_page_num);

    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;

    (*old_num_keys)--;
  }


  *internal_node_right_child(old_node) = *internal_node_child(old_node,*old_num_keys - 1);
  (*old_num_keys)--;


  uint32_t max_after_split = get_node_max_key(table->pager, old_node);

  uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;

  internal_node_insert(table, destination_page_num, child_page_num);
  *node_parent(child) = destination_page_num;

  update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));

  if (!splitting_root) {
    internal_node_insert(table,*node_parent(old_node),new_page_num);
    *node_parent(new_node) = *node_parent(old_node);
  }
}

// 叶子节点分裂并插入
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {


  void* old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void* new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;


  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void* destination_node;
    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    void* destination = leaf_node_cell(destination_node, index_within_node);

    if (i == cursor->cell_num) {
      serialize_row(value,
                    leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
    } else if (i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }


  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
    void* parent = get_page(cursor->table->pager, parent_page_num);

    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
    return;
  }
}

// 在叶子节点插入数据
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
   
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

  if (cursor->cell_num < num_cells) {
   
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

// 执行插入
ExecuteResult execute_insert(Statement* statement, Table* table) {
  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor* cursor = table_find(table, key_to_insert);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  free(cursor);

  return EXECUTE_SUCCESS;
}

// 执行查询
ExecuteResult execute_select(Statement* statement, Table* table) {
  Cursor* cursor = table_start(table);

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

// 执行语句
ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
  }
}

//主函数
int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Table* table = db_open(filename);

  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_NEGATIVE_ID):
        printf("ID must be positive.\n");
        continue;
      case (PREPARE_STRING_TOO_LONG):
        printf("String is too long.\n");
        continue;
      case (PREPARE_SYNTAX_ERROR):
        printf("Syntax error. Could not parse statement.\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_DUPLICATE_KEY):
        printf("Error: Duplicate key.\n");
        break;
    }
  }
}
