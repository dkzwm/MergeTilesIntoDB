#include <cmath>
#include <cstdlib>
#include <dirent.h>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"

#if defined _WIN32 || defined _WIN32 || defined __CYGWIN__
#pragma comment(lib, "Shlwapi.Lib")
#pragma comment(lib, "Rpcrt4.Lib")
#endif

#define NORMAL_COLOR "\x1B[0m"
#define RED "\x1B[31m"
#define BOLD_GREEN "\x1B[32;1m"
#define YELLOW "\x1B[33m"
#define PB "|||||||||||||||||||||||||||||||||||||||"

#define FILE_EXTENSION ".png"

#define ZOOM_KEY_SIZE 1
#define XY_KEY_SIZE 3
#define KEY_SIZE ZOOM_KEY_SIZE + XY_KEY_SIZE + XY_KEY_SIZE
#define PATH_LEN 261
#define MAX_ZOOM 21
#define MAX_FAILED_COUNT 10
#define PROGRESS_MULTIPLIER_NUM 8092

#if defined _WIN32 || defined _WIN32 || defined __CYGWIN__
#define PATH_SEPARATOR "\\"
#else
#define PATH_SEPARATOR "/"
#endif

#ifdef __unix
#define fopen_s(pFile, filename, mode)                                         \
  ((*(pFile)) = fopen((filename), (mode))) == NULL
#endif
using namespace std;
static int compare_fun(const void *p, const void *q);
static int revert_compare_fun(const void *p, const void *q);

void append_xy_key(char *key, int index, int n) {
  int remainder = n;
  for (int i = index; i < index + XY_KEY_SIZE; i++) {
    if (n > 0) {
      float powf = pow(16.0, 4.0 - ((double)i - index) * 2.0);
      int pow = (int)powf;
      key[i] = (char)(remainder / pow);
    } else {
      key[i] = 0;
    }
  }
}

void generate_key(char *key, char zoom, int x, int y) {
  key[0] = zoom;
  append_xy_key(key, ZOOM_KEY_SIZE, x);
  append_xy_key(key, ZOOM_KEY_SIZE + XY_KEY_SIZE, y);
}

int print_progress(int percentage, int last_char_count) {
  int i = 0;
  for (; i < last_char_count; i++) {
    printf("\b");
  }
  int lpad = (int)(percentage * .01f * strlen(PB));
  int rpad = strlen(PB) - lpad;
  i += printf("%3d%% [%.*s%*s]", percentage, lpad, PB, rpad, "");
  fflush(stdout);
  return i;
}

int save_file_2_db(rocksdb::DB *db, char *path, char *key) {
  FILE *file_in;
  int fopen_err = fopen_s(&file_in, path, "rb");
  if (fopen_err != 0) {
    printf("The file \'%s\' is not valid", path);
    return 0;
  }
  fseek(file_in, 0, SEEK_END);
  long file_size = ftell(file_in);
  rewind(file_in);
  char *buffer = (char *)malloc(file_size);
  if (buffer == NULL) {
    printf("Memory Error!!");
    return 0;
  }
  fread(buffer, file_size, 1, file_in);
  rocksdb::Status status =
      db->Put(rocksdb::WriteOptions(), rocksdb::Slice(key, KEY_SIZE),
              rocksdb::Slice(buffer, file_size));
  fclose(file_in);
  if (buffer != NULL) {
    free(buffer);
  }
  buffer = NULL;
  assert(status.ok());
  return 1;
}

int do_put_x(rocksdb::DB *db, vector<char *> xs, char *path, int zoom, int x) {
  printf("%sStart process :%s\n%s", YELLOW, path, NORMAL_COLOR);
  size_t ext_len = strlen(FILE_EXTENSION);
  DIR *dirp;
  struct dirent *dir;
  dirp = opendir(path);
  while ((dir = readdir(dirp)) != NULL) {
    char *d_name = dir->d_name;
    if (strcmp(d_name, ".") == 0 || strcmp(d_name, "..") == 0) {
      continue;
    }
    char *dot = strrchr(d_name, '.');
    if (dot && !strcmp(dot, FILE_EXTENSION)) {
      int is_digit = 1;
      bool checked_dot = false;
      size_t j = 0;
      size_t len = strlen(d_name);
      while (j < len && is_digit && !checked_dot) {
        is_digit = isdigit(d_name[j]);
        checked_dot = d_name[j] == '.';
        j++;
      }
      if (!is_digit && checked_dot) {
        char *file_name = (char *)malloc((len - ext_len + 1) * sizeof(char));
        if (file_name) {
          memset(file_name, '\0', (len - ext_len + 1));
          memcpy(file_name, d_name, (len - ext_len) * sizeof(char));
          xs.push_back(file_name);
        }
      }
    }
  }
  closedir(dirp);
  int size = xs.size();
  if (size > 1) {
    qsort(&xs[0], size, sizeof(char *), compare_fun);
  }
  int old_idx = printf("");
  int idx = old_idx;
  int last_progress = 0;
  float filter_progress_multiplier;
  if (size > PROGRESS_MULTIPLIER_NUM) {
    filter_progress_multiplier = 1.0;
  } else {
    filter_progress_multiplier = 10.0;
  }
  int failed_count = 0;
  char fpath[PATH_LEN];
  char key[KEY_SIZE];
  for (int i = 0; i < size; i++) {
    generate_key(key, zoom, x, atoi(xs[i]));
    memset(fpath, '\0', PATH_LEN);
    sprintf(fpath, "%s%s%s%s", path, PATH_SEPARATOR, xs[i], FILE_EXTENSION);
    failed_count += 1 - save_file_2_db(db, fpath, key);
    if (failed_count > MAX_FAILED_COUNT) {
      break;
    }
    int progress =
        (int)(((double)i + 1) * 100.0 / size / filter_progress_multiplier);
    if (last_progress != progress) {
      idx = print_progress((int)(progress * filter_progress_multiplier), idx);
      last_progress = progress;
    }
  }
  for (int i = 0; i < size; i++) {
    free(xs[i]);
  }
  xs.clear();
  for (int i = old_idx; i < idx; i++) {
    printf("\b");
  }
  int new_idx;
  if (failed_count > MAX_FAILED_COUNT) {
    new_idx = printf("%sFailed process :%s%s", RED, path, NORMAL_COLOR);
  } else {
    new_idx = printf("%sEnd process :%s%s", YELLOW, path, NORMAL_COLOR);
  }
  printf("%*s\n", old_idx - new_idx, "");
  fflush(stdout);
  return failed_count > MAX_FAILED_COUNT ? 0 : 1;
}

int do_put_level(rocksdb::DB *db, vector<char *> levels, char *path, int zoom) {
  printf("%sStart process :%s\n%s", BOLD_GREEN, path, NORMAL_COLOR);
  DIR *dirp;
  struct dirent *dir;
  dirp = opendir(path);
  while ((dir = readdir(dirp)) != NULL) {
    char *d_name = dir->d_name;
    if (strcmp(d_name, ".") == 0 || strcmp(d_name, "..") == 0) {
      continue;
    }
    int is_digit = 1;
    int j = 0;
    size_t len = strlen(d_name);
    while (j < len && is_digit) {
      is_digit = isdigit(d_name[j]);
      j++;
    }
    if (is_digit) {
      char *file_name = (char *)malloc((strlen(d_name) + 1) * sizeof(char));
      if (file_name) {
        sprintf(file_name, "%s", dir->d_name);
        levels.push_back(file_name);
      }
    }
  }
  closedir(dirp);
  int size = levels.size();
  if (size > 1) {
    qsort(&levels[0], size, sizeof(char *), compare_fun);
  }
  int failed_flag = 0;
  char x_path[PATH_LEN];
  vector<char *> xs;
  for (int i = 0; i < size; i++) {
    memset(x_path, '\0', PATH_LEN);
    sprintf(x_path, "%s%s%s", path, PATH_SEPARATOR, levels[i]);
    failed_flag = 1 - do_put_x(db, xs, x_path, zoom, atoi(levels[i]));
    printf("-------------------------------------\n");
    if (failed_flag == 1) {
      break;
    }
  }
  xs.shrink_to_fit();
  for (int i = 0; i < size; i++) {
    free(levels[i]);
  }
  levels.clear();
  printf("%sEnd process :%s\n%s", BOLD_GREEN, path, NORMAL_COLOR);
  return failed_flag == 1 ? 0 : 1;
}

int main(int argc, char **argv) {
  string output_dir;
  string input_dir;
  if (argc != 3) {
    while (output_dir.length() == 0) {
      printf("Please input the output dir:\n");
      getline(cin, output_dir);
    }
    while (input_dir.length() == 0) {
      printf("Please input the input dir:\n");
      getline(cin, input_dir);
    }
  } else {
    output_dir = argv[1];
    input_dir = argv[2];
  }
  rocksdb::DB *db;
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 2LL * 1024 * 1024 * 1024;
  options.target_file_size_base = 2LL * 1024 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 8;
  options.max_bytes_for_level_base = 8LL * 2LL * 1024 * 1024 * 1024;
  options.level_compaction_dynamic_level_bytes = true;
  options.min_write_buffer_number_to_merge = 2;
  options.optimize_filters_for_hits = true;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = 256 * 1024;
  table_options.block_cache = rocksdb::NewLRUCache(1LL * 1024 * 1024 * 1024);
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  rocksdb::Status status = rocksdb::DB::Open(options, output_dir, &db);
  if (!status.code() == rocksdb::Status::Code::kOk) {
    printf("%s\n", status.getState());
  }
  assert(status.ok());
  cout << "Open rocksdb success." << endl;
  char **files = (char **)malloc(MAX_ZOOM * sizeof(char *));
  if (!files) {
    printf("Do malloc memory failed\n");
    exit(1);
  }
  for (register int i = 0; i < MAX_ZOOM; i++) {
    char *buffer = (char *)malloc(10 * sizeof(char));
    if (buffer == NULL) {
      printf("Do malloc memory failed\n");
      return 0;
    }
    files[i] = buffer;
  }
  int count = 0;
  DIR *dirp;
  struct dirent *dir;
  dirp = opendir(input_dir.c_str());
  while ((dir = readdir(dirp)) != NULL && count < MAX_ZOOM) {
    char *d_name = dir->d_name;
    if (strcmp(d_name, ".") == 0 || strcmp(d_name, "..") == 0) {
      continue;
    }
    int is_digit = 1;
    int j = 0;
    size_t len = strlen(d_name);
    while (j < len && is_digit) {
      is_digit = isdigit(d_name[j]);
      j++;
    }
    if (is_digit) {
      sprintf(files[count++], "%s", dir->d_name);
    }
  }
  closedir(dirp);
  if (count == 0) {
    printf("No folders need to be processed");
    exit(1);
  } else {
    printf("There are %d directories to process\n", count);
  }
  if (count > 1) {
    qsort(files, count, sizeof(char *), revert_compare_fun);
  }
  int failed_flag = 0;
  vector<char *> level_files;
  char x_path[PATH_LEN];
  for (int i = 0; i < count; i++) {
    memset(x_path, '\0', PATH_LEN);
    sprintf(x_path, "%s%s%s", input_dir.c_str(), PATH_SEPARATOR, files[i]);
    failed_flag = 1 - do_put_level(db, level_files, x_path, atoi(files[i]));
    printf("=====================================\n");
    if (failed_flag == 1) {
      break;
    }
  }
  int minzoom = atoi(files[count - 1]);
  int maxzoom = atoi(files[0]);
  for (register int i = 0; i < MAX_ZOOM; i++) {
    free(files[i]);
  }
  free(files);
  level_files.shrink_to_fit();
  char begin_key[KEY_SIZE] = {'\0'};
  char end_key[KEY_SIZE] = {'\0'};
  generate_key(begin_key, minzoom, 0, 0);
  generate_key(end_key, maxzoom, (int)pow(2, maxzoom) - 1,
               (int)pow(2, maxzoom) - 1);
  rocksdb::Slice begin(begin_key);
  rocksdb::Slice end(end_key);
  rocksdb::CompactRangeOptions range_options;
  db->CompactRange(range_options, &begin, &end);
  delete db;
}

static int compare_fun(const void *p, const void *q) {
  char *l = *(char **)p;
  char *r = *(char **)q;
  int il = atoi(l);
  int ir = atoi(r);
  if (il == ir) {
    return 0;
  } else if (il > ir) {
    return 1;
  } else {
    return -1;
  }
}

static int revert_compare_fun(const void *p, const void *q) {
  return -compare_fun(p, q);
}