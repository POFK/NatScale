import os
import tomllib
from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize

from setuptools.command.build_ext import build_ext
import subprocess


class CustomBuildExt(build_ext):
    def run(self):
        # 先执行原始编译流程生成.so文件
        super().run()

        # 遍历所有生成的扩展文件
        for output in self.get_outputs():
            if output.endswith(".so"):
                # 移除调试符号
                self.strip_symbols(output)
                # 使用UPX压缩
                self.compress_with_upx(output)

    def strip_symbols(self, file_path):
        """使用strip命令移除调试符号"""
        try:
            print(f"Stripping debug symbols from {file_path}")
            subprocess.check_call(["strip", "--strip-all", file_path])
        except subprocess.CalledProcessError as e:
            print(f"Strip failed: {e}")
        except FileNotFoundError:
            print(
                "Error: 'strip' command not found. Ensure it is installed and in PATH."
            )

    def compress_with_upx(self, file_path):
        """使用UPX进行压缩加壳"""
        try:
            print(f"Compressing {file_path} with UPX")
            subprocess.check_call(["upx", "--best", file_path])
        except subprocess.CalledProcessError as e:
            print(f"UPX compression failed: {e}")
        except FileNotFoundError:
            print("Error: 'upx' command not found. Ensure it is installed and in PATH.")


with open("pyproject.toml", "rb") as f:
    config = tomllib.load(f)
pyproj_cfg = config.get("tool", {}).get("builder-opt", {})
core_modules_path = pyproj_cfg["cython_modules_path"]
core_modules_exclude_py = [
    i.removeprefix("src/").replace("/", ".") for i in core_modules_path
]


def find_cython_modules(base_path):
    modules = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".py"):
                module_path = os.path.join(root, file)
                module_name = module_path.replace(".py", "").replace("/", ".")
                modules.append(
                    Extension(
                        name=module_name.replace("src.", ""),
                        sources=[module_path],
                        extra_compile_args=["-O3"],
                    )
                )
    return modules


core_modules = []
for path in pyproj_cfg["cython_modules_path"]:
    core_modules += find_cython_modules(path)

setup(
    use_scm_version=True,
    ext_modules=cythonize(
        core_modules,
        compiler_directives={"language_level": "3", "embedsignature": True},
        nthreads=4,
    ),
    package_dir={"": "src"},
    packages=find_packages(where="src", exclude=core_modules_exclude_py),
    include_package_data=False,
    exclude_package_data={
        "natscale": [
            "*.c",
        ]
    },
    # This customized cmdclass will generate smaller and safer so file.
    # However, it is not fully tested. Please use this feature with caution
    cmdclass={
        "build_ext": CustomBuildExt,
    },
    zip_safe=False,
)
