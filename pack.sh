#!/bin/bash
set -uo pipefail

# 通用打包工具：将二进制文件及其所有依赖库打包为可移植 tar.gz
# 用法: ./pack.sh <binary_path> [output_tar] [--add file1 file2 ...]

# 出错时打印位置并退出
die() {
    echo "错误: $*" >&2
    exit 1
}

# ERR trap: 任何命令失败时报告行号
trap 'echo "错误: 脚本在第 $LINENO 行失败 (退出码 $?)" >&2' ERR

print_usage() {
    cat << 'EOF'
用法: pack.sh <binary_path> [output_tar] [--add file1 file2 ...]

参数:
  binary_path    要打包的二进制文件路径（必需）
  output_tar     输出的 tar.gz 文件路径（可选，默认: <binary_name>_portable.tar.gz）

选项:
  --add <files>  添加额外的文件或目录到 extra/ 目录中
                 --add 后面的所有参数都被视为要添加的文件
  -h, --help     显示此帮助信息

示例:
  ./pack.sh /usr/bin/gdb
  ./pack.sh ./build/bin/tendisplus ./tendisplus.tar.gz
  ./pack.sh ./myapp ./myapp.tar.gz --add config.yaml scripts/

打包结构:
  <name>.sh          启动脚本（顶层）
  <name>/            同名子目录
  ├── <name>         二进制文件
  ├── lib/           依赖库（含动态链接器）
  └── extra/         额外文件（仅当使用 --add 时）

使用:
  tar xzf <name>_portable.tar.gz -C /some/path
  cd /some/path
  ./<name>.sh [参数...]
EOF
    exit 0
}

# ============================================================
# 参数解析
# ============================================================

BINARY_PATH=""
OUTPUT_TAR=""
EXTRA_FILES=()
PARSING_EXTRA=0

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            print_usage
            ;;
        --add)
            PARSING_EXTRA=1
            shift
            ;;
        --)
            shift
            if [[ $PARSING_EXTRA -eq 1 ]]; then
                while [[ $# -gt 0 ]]; do
                    EXTRA_FILES+=("$1")
                    shift
                done
            else
                while [[ $# -gt 0 ]]; do
                    if [[ -z "$BINARY_PATH" ]]; then
                        BINARY_PATH="$1"
                    elif [[ -z "$OUTPUT_TAR" ]]; then
                        OUTPUT_TAR="$1"
                    else
                        die "多余的参数: $1 (如需添加额外文件，请使用 --add)"
                    fi
                    shift
                done
            fi
            ;;
        -*)
            if [[ $PARSING_EXTRA -eq 1 ]]; then
                EXTRA_FILES+=("$1")
            else
                die "未知选项: $1 (使用 -h 查看帮助)"
            fi
            shift
            ;;
        *)
            if [[ $PARSING_EXTRA -eq 1 ]]; then
                EXTRA_FILES+=("$1")
            elif [[ -z "$BINARY_PATH" ]]; then
                BINARY_PATH="$1"
            elif [[ -z "$OUTPUT_TAR" ]]; then
                OUTPUT_TAR="$1"
            else
                die "多余的参数: $1 (如需添加额外文件，请使用 --add)"
            fi
            shift
            ;;
    esac
done

# ============================================================
# 参数验证
# ============================================================

[[ -z "$BINARY_PATH" ]] && die "必须指定二进制文件路径 (使用 -h 查看帮助)"
[[ ! -f "$BINARY_PATH" ]] && die "二进制文件不存在: $BINARY_PATH"

if [[ ! -x "$BINARY_PATH" ]]; then
    echo "警告: 二进制文件没有执行权限，已自动添加" >&2
    chmod +x "$BINARY_PATH"
fi

BINARY_NAME=$(basename "$BINARY_PATH")

if [[ -z "$OUTPUT_TAR" ]]; then
    OUTPUT_TAR="./${BINARY_NAME}_portable.tar.gz"
fi

# ============================================================
# 临时工作目录
# ============================================================

TEMP_DIR=$(mktemp -d /tmp/pack.XXXXXX) || die "无法创建临时目录"
trap 'rm -rf "$TEMP_DIR"' EXIT

# 结构：启动脚本在顶层，$BINARY_NAME/ 子目录放二进制和库
PACK_DIR="$TEMP_DIR/$BINARY_NAME"
LIB_DIR="$PACK_DIR/lib"
mkdir -p "$LIB_DIR"

echo "=========================================="
echo "二进制文件: $BINARY_PATH"
echo "输出文件:   $OUTPUT_TAR"
if [[ ${#EXTRA_FILES[@]} -gt 0 ]]; then
    echo "额外文件:   ${EXTRA_FILES[*]}"
fi
echo "=========================================="
echo ""

# ============================================================
# GLIBC 版本检查
# ============================================================

if command -v readelf >/dev/null 2>&1; then
    max_glibc=$(readelf -V "$BINARY_PATH" 2>/dev/null \
                | grep -oE 'GLIBC_[0-9.]+' | sort -V | tail -1 || true)
    if [[ -n "$max_glibc" ]]; then
        echo "GLIBC 要求: $max_glibc"
    fi
fi

# ============================================================
# 静态链接检测
# ============================================================

is_static=0
ldd_output=$(ldd "$BINARY_PATH" 2>&1 || true)

if [[ -z "$ldd_output" ]] || echo "$ldd_output" | grep -q "not a dynamic executable"; then
    is_static=1
    echo "检测到静态链接，跳过库收集"
fi

# ============================================================
# 复制二进制
# ============================================================

cp "$BINARY_PATH" "$PACK_DIR/" || die "复制二进制文件失败"
chmod +x "$PACK_DIR/$BINARY_NAME"
echo "已复制: $BINARY_NAME"

# ============================================================
# 收集依赖库
# ============================================================

if [[ $is_static -eq 0 ]]; then
    echo ""
    echo "收集依赖库..."

    # 动态链接器
    interpreter_path=""
    if command -v readelf >/dev/null 2>&1; then
        interpreter_path=$(readelf -l "$BINARY_PATH" 2>/dev/null \
            | grep -oP '(?<=Requesting program interpreter: )[^\]]+' || true)
    fi
    if [[ -z "$interpreter_path" ]]; then
        interpreter_path=$(echo "$ldd_output" | awk '/ld-linux/ {print $1; exit}')
    fi

    if [[ -n "$interpreter_path" && -f "$interpreter_path" ]]; then
        echo "  动态链接器: $interpreter_path"
        cp -L "$interpreter_path" "$LIB_DIR/" || die "复制动态链接器失败"
        chmod +x "$LIB_DIR/$(basename "$interpreter_path")"
    else
        echo "  警告: 未能识别动态链接器" >&2
    fi

    # 依赖库
    lib_paths=$(echo "$ldd_output" | awk '
        /=>/ && $3 ~ /^\// { print $3 }
        /^\t\// && !/=>/ { print $1 }
    ' | grep -v 'linux-vdso' | sort -u || true)

    lib_count=0
    while IFS= read -r lib_path; do
        [[ -z "$lib_path" ]] && continue
        if [[ ! -f "$lib_path" ]]; then
            echo "  警告: 库文件不存在: $lib_path" >&2
            continue
        fi
        lib_name=$(basename "$lib_path")
        [[ -f "$LIB_DIR/$lib_name" ]] && continue

        cp -L "$lib_path" "$LIB_DIR/" || die "复制库失败: $lib_path"
        lib_count=$((lib_count + 1))
    done <<< "$lib_paths"

    echo "  已复制 $lib_count 个依赖库"
fi

# ============================================================
# 额外文件
# ============================================================

if [[ ${#EXTRA_FILES[@]} -gt 0 ]]; then
    echo ""
    echo "复制额外文件..."
    EXTRA_DIR="$PACK_DIR/extra"
    mkdir -p "$EXTRA_DIR"

    for item in "${EXTRA_FILES[@]}"; do
        if [[ -e "$item" ]]; then
            cp -rL "$item" "$EXTRA_DIR/"
            echo "  已添加: $item"
        else
            echo "  警告: 不存在，跳过: $item" >&2
        fi
    done
fi

# ============================================================
# 启动脚本（放在顶层）
# ============================================================

# 启动脚本放在顶层（TEMP_DIR 根目录）
cat > "$TEMP_DIR/${BINARY_NAME}.sh" << 'LAUNCHER_EOF'
#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="$SCRIPT_DIR/@@BINARY_NAME@@/lib"
BINARY="$SCRIPT_DIR/@@BINARY_NAME@@/@@BINARY_NAME@@"

if [[ ! -x "$BINARY" ]]; then
    echo "错误: 二进制文件不存在或不可执行: $BINARY" >&2
    exit 1
fi

LD_LINUX=""
shopt -s nullglob
for ld in "$LIB_DIR"/ld-linux*.so.* "$LIB_DIR"/ld-musl*.so.*; do
    if [[ -x "$ld" && -f "$ld" ]]; then
        LD_LINUX="$ld"
        break
    fi
done
shopt -u nullglob

if [[ -n "$LD_LINUX" ]]; then
    exec "$LD_LINUX" --library-path "$LIB_DIR" "$BINARY" "$@"
else
    export LD_LIBRARY_PATH="$LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    exec "$BINARY" "$@"
fi
LAUNCHER_EOF

sed -i "s/@@BINARY_NAME@@/$BINARY_NAME/g" "$TEMP_DIR/${BINARY_NAME}.sh"
chmod +x "$TEMP_DIR/${BINARY_NAME}.sh"

# ============================================================
# 打包（扁平结构，无多余顶级目录）
# ============================================================

# 打包：启动脚本 + 同名子目录
echo ""
echo "创建压缩包..."
tar czf "$OUTPUT_TAR" -C "$TEMP_DIR" "${BINARY_NAME}.sh" "$BINARY_NAME" || die "创建 tar 包失败"

size=$(du -h "$OUTPUT_TAR" 2>/dev/null | cut -f1 || echo "unknown")

echo ""
echo "=========================================="
echo "打包完成: $OUTPUT_TAR ($size)"
echo "=========================================="
echo ""
echo "使用方法:"
echo "  tar xzf $(basename "$OUTPUT_TAR") -C <目标目录>"
echo "  cd <目标目录>"
echo "  ./${BINARY_NAME}.sh [参数...]"
