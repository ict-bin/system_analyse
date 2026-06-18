r"""func_extract 回归测试 —— 重点：杜绝正则灾难性回溯导致的 runner 卡死。

历史故障：_src_summ / _C_FUNC_RE 的 `(?:\w+(?:\s*::\s*)?)+` 等嵌套量词正则在病态源码
输入上指数级回溯，单线程钉满 CPU、持有 GIL、饿死注册心跳 → runner 僵死。

本测试保证：
  1. 病态输入（长 \w 串）在毫秒级返回（线性，不回溯）。
  2. C/C++（tree-sitter 或降级正则）/ shell / python 函数名提取正确。
"""
import time
import unittest

from app.pipeline.func_extract import (
    extract_functions,
    extract_function_names,
    extract_cpp_functions,
    extract_shell_functions,
    extract_python_functions,
    _extract_cpp_functions_fallback,
)


class TestNoCatastrophicBacktracking(unittest.TestCase):
    def test_pathological_input_is_linear(self):
        # 60KB 长 \w 串 + 非 \w 结尾：旧正则 28 字符即需 17s，新实现必须毫秒级
        bad = ("a" * 60000) + "!"
        for fn, name in (
            (lambda c: extract_cpp_functions(c), "cpp"),
            (lambda c: _extract_cpp_functions_fallback(c), "cpp_fallback"),
            (lambda c: extract_shell_functions(c), "shell"),
            (lambda c: extract_python_functions(c), "python"),
        ):
            t = time.time()
            fn(bad)
            dt = time.time() - t
            self.assertLess(dt, 1.0, f"{name} 在病态输入上耗时 {dt:.2f}s（疑似回溯）")

    def test_pathological_many_lines(self):
        # 大量病态长行
        content = "\n".join(("x" * 1500 + "::" + "y" * 1500) for _ in range(200))
        t = time.time()
        extract_function_names("a.cpp", content)
        self.assertLess(time.time() - t, 1.0)


class TestExtraction(unittest.TestCase):
    def test_cpp_fallback(self):
        src = "int foo(int a){return a;}\nvoid Bar::baz(){}\nint x = call_me(1);"
        names = [f["name"] for f in _extract_cpp_functions_fallback(src)]
        self.assertIn("foo", names)
        self.assertNotIn("call_me", names)  # 调用不算定义（前面无类型 token 的行才会误判）

    def test_cpp_dispatch(self):
        src = "static inline T* mk(void){ return 0; }\nint api(int);"
        names = extract_function_names("h.cpp", src)
        self.assertTrue(any("mk" in n for n in names))

    def test_shell(self):
        sh = "do_start() {\n  echo hi\n}\nfunction cleanup {\n  rm -f /tmp/x\n}\n"
        fns = extract_shell_functions(sh)
        names = [f["name"] for f in fns]
        self.assertEqual(set(names), {"do_start", "cleanup"})
        # 函数体已提取
        body_map = {f["name"]: f["body"] for f in fns}
        self.assertIn("echo hi", body_map["do_start"])
        self.assertIn("rm -f", body_map["cleanup"])

    def test_python(self):
        py = "def top(a, b):\n    return a + b\n\nclass C:\n    def method(self):\n        pass\n"
        fns = extract_python_functions(py)
        names = [f["name"] for f in fns]
        self.assertEqual(set(names), {"top", "method"})
        body_map = {f["name"]: f["body"] for f in fns}
        self.assertIn("return a + b", body_map["top"])

    def test_dispatch_by_ext(self):
        self.assertEqual(extract_function_names("x.txt", "def a(): pass"), [])
        self.assertEqual(extract_function_names("x.py", "def a():\n    pass"), ["a"])
        self.assertEqual(extract_function_names("x.sh", "a() {\n :\n}"), ["a"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
