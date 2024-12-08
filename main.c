#define FUSE_USE_VERSION 31
#include <errno.h>
#include <fcntl.h>
#include <fuse3/fuse.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BLOCK_SIZE 512
#define BLOCKS_NUM 2880
#define INODES_NUM 192
#define MAX_NAME_LEN 256
#define MAX_FILE_SIZE (BLOCK_SIZE * 14) 
#define FS_IMAGE_PATH "filesystem.img"
#define ROOT_INODE 0

typedef struct inode {
  mode_t mode;
  char name[MAX_NAME_LEN]; 
  size_t size;
  int links_count;
  int blocks[14];
} inode_t;

unsigned char block_bitmap[BLOCKS_NUM];
unsigned char inode_bitmap[INODES_NUM];
inode_t inodes_array[INODES_NUM] = {0};
int fs_image_fd;

int get_word_after_last_slash(const char *path, char *result) {
  const char *last_slash = strrchr(path, '/');
  if (last_slash) {
    strcpy(result, last_slash + 1);
  } else {
    strcpy(result, path);
  }
  return 0;
}

int find_free_bit(const unsigned char *bitmap, int size) {
  for (int i = 0; i < size; ++i) {
    if (bitmap[i] == 0)
      return i;
  }
  return -1;
}

bool init_filesystem_image() {
  fs_image_fd = open(FS_IMAGE_PATH, O_RDWR | O_CREAT, 0644);
  if (fs_image_fd < 0) {
    perror("Error opening filesystem image");
    exit(EXIT_FAILURE);
  }
  if (lseek(fs_image_fd, 0, SEEK_END) == 0) {
    memset(block_bitmap, 0, sizeof(block_bitmap));
    memset(inode_bitmap, 0, sizeof(inode_bitmap));
    inode_bitmap[ROOT_INODE] = 1;
    inode_t root_inode = {.mode = S_IFDIR | 0755, .size = 0, .links_count = 2};
    strcpy(root_inode.name, "/");
    inodes_array[ROOT_INODE] = root_inode;
    pwrite(fs_image_fd, block_bitmap, sizeof(block_bitmap), 0);
    pwrite(fs_image_fd, inode_bitmap, sizeof(inode_bitmap), sizeof(block_bitmap));
    pwrite(fs_image_fd, inodes_array, sizeof(inodes_array), sizeof(block_bitmap) + sizeof(inode_bitmap));
    unsigned char empty_block[BLOCK_SIZE] = {0};
    for (int i = 0; i < BLOCKS_NUM; ++i) {
        pwrite(fs_image_fd, empty_block, BLOCK_SIZE,
               sizeof(block_bitmap) + sizeof(inode_bitmap) + sizeof(inodes_array) + i * BLOCK_SIZE);
    }
    return false;
  }
  pread(fs_image_fd, block_bitmap, sizeof(block_bitmap), 0);
  pread(fs_image_fd, inode_bitmap, sizeof(inode_bitmap), sizeof(block_bitmap));
  pread(fs_image_fd, inodes_array, sizeof(inodes_array), sizeof(block_bitmap) + sizeof(inode_bitmap));
  return true;
}

void sync_block_bitmap() {
  pwrite(fs_image_fd, block_bitmap, sizeof(block_bitmap), 0);
}

void sync_inode_bitmap() {
  pwrite(fs_image_fd, inode_bitmap, sizeof(inode_bitmap), sizeof(block_bitmap));
}

void sync_inodes() {
  pwrite(fs_image_fd, inodes_array, sizeof(inodes_array), sizeof(block_bitmap) + sizeof(inode_bitmap));
}

static int get_inode_index(const char *name) {
  for (int i = 0; i < INODES_NUM; ++i)
    if (inode_bitmap[i] && strcmp(inodes_array[i].name, name) == 0)
      return i;
  return -1;
}

int num_of_chars(const char *str, const char c) {
  int counter = 0;
  int len = strlen(str);
  for (int i = 0; i < len - 1; i++) {
    if (str[i] == c) {
      counter++;
    }
  }
  return counter;
}

char **get_dirs_from_path(const char *path, int *n) {
  *n = 0;
  if (strcmp(path, "/") == 0 || strlen(path) == 0) {
    return NULL;
  }
  int len = strlen(path);
  char path_cpy[len + 1];
  strcpy(path_cpy, path);
  *n = num_of_chars(path_cpy, '/');
  if (path_cpy[len - 1] == '/') {
    *n = *n - 1;
  }
  int i = 0;
  char **ret = malloc(*n);
  char *dir = strtok(path_cpy, "/");
  while (dir && i < *n) {
    int dir_len = strlen(dir);
    ret[i] = malloc(dir_len + 1);
    memcpy(ret[i], dir, dir_len);
    ret[i][dir_len] = '\0';
    i++;
    dir = strtok(NULL, "/");
  }
  return ret;
}

static int myfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
  printf("\033[5;33mCALLED GETATTR\033[0m\n");
  (void)fi;
  memset(stbuf, 0, sizeof(struct stat));
  if (strcmp(path, "/") == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return 0;
  }
  int n;
  off_t offset = -1;
  for (int i = 0; i < INODES_NUM; ++i) {
    if (strcmp(inodes_array[i].name, path) == 0) {
      offset = i;
      break;
    }
  }
  if (offset == -1) {
    return -ENOENT;
  }
  stbuf->st_mode = inodes_array[offset].mode;
  stbuf->st_nlink = inodes_array[offset].links_count;
  stbuf->st_size = inodes_array[offset].size;
  return 0;
}

static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
  printf("\033[5;33mCALLED READDIR\033[0m\n");
  const char *name = strrchr(path, '/') + 1;
  int inode_index = get_inode_index(name[0] ? name : "/");
  if (inode_index == -1)
    return -ENOENT;
  filler(buf, ".", NULL, 0, 0);
  filler(buf, "..", NULL, 0, 0);
  for (int i = 0; i < INODES_NUM; ++i) {
    if (inode_bitmap[i] &&
        strncmp(inodes_array[i].name, path, strlen(path)) == 0) {
      char tmp[MAX_NAME_LEN] = {0};
      if (strcmp(inodes_array[i].name, "/") == 0) {
        continue;
      }
      get_word_after_last_slash(inodes_array[i].name, tmp);
      filler(buf, tmp, NULL, 0, 0);
    }
  }
  return 0;
}


static int myfs_mkdir(const char *path, mode_t mode) {
  printf("\033[5;33mCALLED MKDIR\033[0m\n");
  int free_inode = find_free_bit(inode_bitmap, INODES_NUM);
  if (free_inode == -1)
    return -ENOSPC;
  inode_bitmap[free_inode] = 1;
  inodes_array[free_inode].mode = S_IFDIR | mode;
  inodes_array[free_inode].size = 0;
  inodes_array[free_inode].links_count = 2;
  strcpy(inodes_array[free_inode].name, path);
  sync_inode_bitmap();
  sync_inodes();
  sync_block_bitmap();
  return 0;
}

static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
  printf("\033[5;33mCALLED CREATE\033[0m\n");
  if (get_inode_index(path) != -1)
      return -EEXIST; 
  int free_inode = find_free_bit(inode_bitmap, INODES_NUM);
  if (free_inode == -1)
      return -ENOSPC; 
  int free_block = find_free_bit(block_bitmap, BLOCKS_NUM);
  if (free_block == -1)
      return -ENOSPC; 
  inode_bitmap[free_inode] = 1;
  block_bitmap[free_block] = 1;
  inode_t new_file = {.mode = S_IFREG | mode, .size = 0, .links_count = 1};
  strcpy(new_file.name, path);
  memset(new_file.blocks, 0, sizeof(new_file.blocks));
  new_file.blocks[0] = free_block;
  inodes_array[free_inode] = new_file;
  sync_inode_bitmap();
  sync_block_bitmap();
  sync_inodes();
  fi->fh = free_inode;
  return 0;
}

int find_free_block(){
  for (int i = 0; i < BLOCKS_NUM; ++i)   
      if (block_bitmap[i] == 0)
        return i;
  return -1;
}

static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  printf("\033[5;33mCALLED WRITE\033[0m\n");
  if (!buf || size == 0)
      return -EINVAL;
  if (fs_image_fd < 0){
      perror("Failed to open virtual disk file");
      return -EIO;
  }
  int inode_index = get_inode_index(path);
  if (inode_index == -1) {
      return -ENOENT; 
  }
  inode_t *file_inode = &inodes_array[inode_index];
  if (fi->flags & O_APPEND) {
      offset = file_inode->size;
  }
  size_t bytes_written = 0;
  size_t bytes_remaining = size;
  const char *current_buf = buf;
  while (bytes_remaining > 0) {
      int block_index = offset / BLOCK_SIZE;
      if (block_index >= 14)
          return -EFBIG; 
      if (file_inode->blocks[block_index] == 0) {
          int new_block_index = find_free_block();
          if (new_block_index == -1) {
              return -ENOSPC; 
          }
          block_bitmap[new_block_index] = 1;
          file_inode->blocks[block_index] = new_block_index;
      }
      off_t block_offset_in_file = sizeof(block_bitmap) + sizeof(inode_bitmap) + sizeof(inodes_array) + file_inode->blocks[block_index] * BLOCK_SIZE;
      size_t block_offset = offset % BLOCK_SIZE;
      size_t available_space = BLOCK_SIZE - block_offset;
      size_t to_write = (bytes_remaining < available_space) ? bytes_remaining : available_space;
      ssize_t written = pwrite(fs_image_fd, current_buf, to_write, block_offset_in_file + block_offset);
      if (written == -1) {
          perror("Error during pwrite");
          return -EIO;
      }
      current_buf += written;
      bytes_written += written;
      bytes_remaining -= written;
      offset += written;
  }
  if (offset > file_inode->size) {
      file_inode->size = offset;
  }
  sync_inode_bitmap();
  sync_inodes();
  sync_block_bitmap();
  return bytes_written; 
}

static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  printf("\033[5;33mCALLED READ\033[0m\n");
  off_t old_offset = offset;
  if (!buf || size == 0) {
      return -EINVAL; 
  }
  if (fs_image_fd < 0) {
      perror("Failed to open virtual disk file");
      return -EIO;
  }
  int inode_index = get_inode_index(path);
  if (inode_index == -1) {
      return -ENOENT; 
  }
  inode_t *file_inode = &inodes_array[inode_index];
  if (offset >= file_inode->size) {
      return 0; 
  }
  size_t bytes_read = 0;
  size_t bytes_remaining = size;
  size_t file_size = file_inode->size;
  while (bytes_remaining > 0) {
      int block_index = offset / BLOCK_SIZE;
      if (block_index >= 14)
          break;
      inode_t *file_inode = &inodes_array[inode_index];
      int block_offset_in_file = sizeof(block_bitmap) + sizeof(inode_bitmap) + sizeof(inodes_array) + file_inode->blocks[block_index] * BLOCK_SIZE;
      size_t block_offset = offset % BLOCK_SIZE;
      size_t available_to_read = BLOCK_SIZE - block_offset;
      size_t to_read = bytes_remaining < available_to_read ? bytes_remaining : available_to_read;
      ssize_t read_result = pread(fs_image_fd, buf + bytes_read, to_read, block_offset_in_file + block_offset);
      if (read_result == -1) {
          perror("Failed to read from virtual disk file");
          return -EIO;
      }
      bytes_read += read_result;
      bytes_remaining -= read_result;
      offset += read_result;
  }
  offset = old_offset;
  return bytes_read; 
}

static int is_block_empty(const char *block_data) {
    for (int i = 0; i < BLOCK_SIZE; i++) {
        if (block_data[i] != 0)
            return 0; 
    }
    return 1;
}

void read_block(int block_number, void *buffer) {
    lseek(fs_image_fd, BLOCK_SIZE * block_number, SEEK_SET);
    read(fs_image_fd, buffer, BLOCK_SIZE);
}

static int myfs_unlink(const char *path) {
  printf("\033[5;31mCALLED UNLINK\033[0m\n");
  int inode_index = get_inode_index(path);
  if (inode_index == -1)
      return -ENOENT; 
  inode_t *file_inode = &inodes_array[inode_index];
  if (!S_ISREG(file_inode->mode))
      return -EISDIR; 
  for (int i = 0; i < BLOCKS_NUM && file_inode->blocks[i] != 0; i++)
      block_bitmap[file_inode->blocks[i]] = 0;
  memset(file_inode, 0, sizeof(inode_t));
  inode_bitmap[inode_index] = 0;
  sync_block_bitmap();
  sync_inode_bitmap();
  sync_inodes();
  return 0;
}

static int myfs_rmdir(const char *path) {
  printf("\033[5;34mCALLED RMDIR\033[0m\n");
  int inode_index = get_inode_index(path);
  if (inode_index == -1)
      return -ENOENT; 
  inode_t *dir_inode = &inodes_array[inode_index];
  if (!S_ISDIR(dir_inode->mode))
      return -ENOTDIR; 
  for (int i = 0; i < BLOCKS_NUM && dir_inode->blocks[i] != 0; i++) {
      char block_data[BLOCK_SIZE];
      read_block(dir_inode->blocks[i], block_data);
      if (!is_block_empty(block_data))
          return -ENOTEMPTY; 
  }
  for (int i = 0; i < BLOCKS_NUM && dir_inode->blocks[i] != 0; i++)
      block_bitmap[dir_inode->blocks[i]] = 0;
  memset(dir_inode, 0, sizeof(inode_t));
  inode_bitmap[inode_index] = 0;
  sync_block_bitmap();
  sync_inode_bitmap();
  sync_inodes();
  return 0;
}

static const struct fuse_operations myfs_operations = {
    .getattr = myfs_getattr,
    .readdir = myfs_readdir,
    .mkdir = myfs_mkdir,
    .rmdir = myfs_rmdir,
    .unlink = myfs_unlink,
    .create = myfs_create,
    .write = myfs_write,
    .read = myfs_read,
};

int main(int argc, char *argv[]) {
  init_filesystem_image();
  return fuse_main(argc, argv, &myfs_operations, NULL);
}

