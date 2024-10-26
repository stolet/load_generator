TARGET = loadgen
BUILD_DIR = build

CC = gcc

CFLAGS = -Wall -Wextra -O3
LDFLAGS = -lm -lpthread

SRC = loadgen.c
OBJ = $(BUILD_DIR)/$(SRC:.c=.o)

all: $(BUILD_DIR) $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $(BUILD_DIR)/$(TARGET) $(OBJ) $(LDFLAGS)

$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean

