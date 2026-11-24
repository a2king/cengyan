"""从 pycengyan 的错误码映射生成 Java SDK 的契约用例，保证两个 SDK 对同一错误码抛同一类异常。

不参与 Maven 构建；pycengyan 的映射变更后手动重跑（见 contract-gen/README.md）。

用法：PYTHONPATH=<cengyan-cli-python>/src python -I generate_error_codes.py <输出文件>
（-I 下 PYTHONPATH 不生效，脚本用 sys.argv[2] 接收 src 目录）
"""
import json
import sys

sys.path.insert(0, sys.argv[2])
from pycengyan import exceptions  # noqa: E402

# Python 类名 → Java 类名
RENAMES = {
    "CengyanError": "CengyanException",
    "CengyanConnectionError": "ConnectionException",
    "GqlSyntaxError": "GqlSyntaxException",
}


def java_name(cls):
    name = cls.__name__
    return RENAMES.get(name, name[:-len("Error")] + "Exception" if name.endswith("Error") else name)


codes = sorted(set(exceptions._ERRORS) | set(exceptions._UNCERTAIN_FOR_WRITES) | {"SOMETHING_NEW"})
cases = []
for code in codes:
    cases.append({
        "code": code,
        "read": java_name(type(exceptions.error_for_response({"code": code}, write=False))),
        "write": java_name(type(exceptions.error_for_response({"code": code}, write=True))),
        "unknown": java_name(type(exceptions.error_for_response({"code": code}))),
    })
document = {"generator": "contract-gen/generate_error_codes.py (pycengyan.exceptions)", "cases": cases}
with open(sys.argv[1], "w", encoding="utf-8", newline="\n") as stream:
    json.dump(document, stream, ensure_ascii=False, indent=2)
    stream.write("\n")
print("cases", len(cases))
