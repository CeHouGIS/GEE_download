# python /code/GEE_download/unzip_gee_zips.py --image_file_name "Shanghai_time_series"
# python /code/GEE_download/unzip_gee_zips.py --image_file_name "Africa_grid_-15_10_-10_15"

import os
import zipfile
import tarfile
import shutil
import argparse

# --- 先决条件 ---
# 1. 对于 .rar 文件，你需要先安装 'rarfile' 模块
#    在终端或命令行中运行: pip install rarfile
#
# 2. 'rarfile' 模块依赖于你系统中安装的 unrar 命令行工具。
#    - Windows: 从 rarlab.com 下载并安装 WinRAR，确保其路径在系统环境变量中。
#    - macOS: 使用 Homebrew: brew install unrar
#    - Linux (Debian/Ubuntu): sudo apt-get install unrar
#    - Linux (Fedora/CentOS): sudo yum install unrar
try:
    import rarfile
    RAR_SUPPORT = True
except ImportError:
    RAR_SUPPORT = False
    print("警告: 未找到 'rarfile' 模块，将跳过 .rar 文件的解压。")
    print("如需支持，请运行: pip install rarfile")


def decompress_all(source_path, destination_path):
    """
    解压源路径下的所有支持的压缩文件到目标路径。

    :param source_path: 包含压缩文件的源文件夹路径。
    :param destination_path: 解压后文件存放的目标文件夹路径。
    """

    # 1. 检查并创建目标文件夹
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
        print(f"已创建目标文件夹: {destination_path}")

    # 2. 遍历源路径下的所有文件和文件夹
    for root, dirs, files in os.walk(source_path):
        for filename in files:
            file_path = os.path.join(root, filename)

            # 为了避免解压后的文件发生冲突，为每个压缩包创建一个同名的子文件夹
            archive_name = os.path.splitext(filename)[0]
            # 替换文件名中的点，避免创建类似 'file.tar' 这样的文件夹名
            archive_name = archive_name.replace('.', '_')
            output_folder = os.path.join(destination_path, archive_name)

            try:
                # 3. 根据文件后缀名选择不同的解压方法
                if filename.endswith('.zip'):
                    print(f"正在解压 [ZIP]: {file_path}")
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(output_folder)
                    print(f"  -> 成功解压到: {output_folder}")

                elif tarfile.is_tarfile(file_path): # .tar, .tar.gz, .tar.bz2 等
                    print(f"正在解压 [TAR]: {file_path}")
                    with tarfile.open(file_path, 'r:*') as tar_ref:
                        tar_ref.extractall(path=output_folder)
                    print(f"  -> 成功解压到: {output_folder}")

                elif filename.endswith('.rar') and RAR_SUPPORT:
                    print(f"正在解压 [RAR]: {file_path}")
                    with rarfile.RarFile(file_path, 'r') as rar_ref:
                        rar_ref.extractall(path=output_folder)
                    print(f"  -> 成功解压到: {output_folder}")
                
                # 你可以在这里添加对其他格式的支持，比如 .7z (需要 py7zr 库)

            except Exception as e:
                print(f"!!! 解压文件失败: {file_path}")
                print(f"    错误原因: {e}")

    print("\n--- 所有解压任务已完成 ---")


if __name__ == '__main__':
    # --- 请在这里配置你的路径 ---

    # 源路径：存放压缩文件的文件夹
    # Windows 示例: r'C:\Users\YourUser\Downloads\Archives'
    # Linux/macOS 示例: '/home/user/documents/compressed_files'
    SOURCE_DIRECTORY = r'/nas/houce/Alphaearth_embedding/zips/'

    # 目标路径：解压后文件存放的文件夹
    # Windows 示例: r'C:\Users\YourUser\Downloads\Extracted'
    # Linux/macOS 示例: '/home/user/documents/extracted_files'
    parser = argparse.ArgumentParser(description='解压 GEE 压缩包')
    parser.add_argument('--image_file_name', type=str, required=True, help='指定解压后的文件夹名')
    args = parser.parse_args()
    image_file_name = args.image_file_name
    # image_file_name = 'Shanghai_time_series'
    DESTINATION_DIRECTORY = rf'/nas/houce/Alphaearth_embedding/GEE_extracted/{image_file_name}/'
    os.makedirs(DESTINATION_DIRECTORY, exist_ok=True)
    print(f"目标解压路径: {DESTINATION_DIRECTORY}")
    # --- 配置结束 ---

    # 检查路径是否被修改
    if '请替换' in SOURCE_DIRECTORY or '请替换' in DESTINATION_DIRECTORY:
        print("错误：请先在脚本中设置 'SOURCE_DIRECTORY' 和 'DESTINATION_DIRECTORY' 的实际路径！")
    else:
        decompress_all(SOURCE_DIRECTORY, DESTINATION_DIRECTORY)