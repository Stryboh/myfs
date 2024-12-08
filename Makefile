all:
	gcc main.c -o main -D_FILE_OFFSET_BITS=64 -Wall --pedantic -g `pkg-config fuse3 --cflags --libs`

