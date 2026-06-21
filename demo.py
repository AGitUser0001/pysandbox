import argparse
import asyncio
import builtins
import io
import keyword
import os
import shutil
import sys
import textwrap
import time
import tokenize
import traceback
from types import ModuleType
from typing import Any, Protocol

from pysandbox import PythonRuntime, RuntimeLimits


RESET = "\x1b[0m"
BOLD = "\x1b[1m"
CYAN = "\x1b[36m"
DIM = "\x1b[2m"
GREEN = "\x1b[32m"
RED = "\x1b[31m"
YELLOW = "\x1b[33m"
MAGENTA = "\x1b[35m"
WHITE = "\x1b[37m"
TYPEWRITER_DELAY = 0.002
PRESENTATION_PAUSE = 5.0
PRESENTATION_READING_CHARS_PER_SECOND = 42.0
PRESENTATION_MIN_PAUSE = 0.6


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the pysandbox tutorial demo.",
    )
    parser.add_argument(
        "--editor",
        action="store_true",
        help="skip the tutorial and open the host-code playground",
    )
    return parser.parse_args()


def typewrite(text: str, *, end: str = "\n") -> None:
    if not sys.stdout.isatty():
        print(text, end=end)
        return

    index = 0
    while index < len(text):
        if text[index] == "\x1b":
            escape_end = index + 1
            while escape_end < len(text) and text[escape_end] != "m":
                escape_end += 1
            sys.stdout.write(text[index : escape_end + 1])
            index = escape_end + 1
            continue

        sys.stdout.write(text[index])
        sys.stdout.flush()
        time.sleep(TYPEWRITER_DELAY)
        index += 1

    sys.stdout.write(end)
    sys.stdout.flush()


def presentation_pause(seconds: float = PRESENTATION_PAUSE) -> None:
    if not sys.stdout.isatty() or not sys.stdin.isatty():
        return

    interval = seconds / 3
    sys.stdout.write(DIM)
    sys.stdout.flush()
    skipped = False

    for _ in range(3):
        sys.stdout.write(".")
        sys.stdout.flush()
        if wait_for_enter(interval):
            skipped = True
            break

    sys.stdout.write(RESET + "\n")
    sys.stdout.flush()

    if skipped:
        return


def wait_for_enter(seconds: float) -> bool:
    if sys.platform == "win32":
        try:
            import msvcrt
        except ImportError:
            time.sleep(seconds)
            return False

        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if msvcrt.kbhit():
                key = msvcrt.getwch()
                if key in {"\r", "\n"}:
                    return True
            time.sleep(0.05)
        return False

    try:
        import select
    except ImportError:
        time.sleep(seconds)
        return False

    if select.select([sys.stdin], [], [], seconds)[0]:
        sys.stdin.readline()
        return True

    return False


def create_runtime() -> PythonRuntime:
    runtime = PythonRuntime(
        limits=RuntimeLimits(
            fuel=10_000_000_000,
            replenish_fuel_interval=30,
        )
    )

    @runtime.rpc.expose
    def add(a: int, b: int) -> int:
        return int(a) + int(b)

    @runtime.rpc.expose("host_upper")
    def uppercase(text: str) -> str:
        return text.upper()

    return runtime


def host_setup_example() -> str:
    return textwrap.dedent(
        """
        from pysandbox import PythonRuntime, RuntimeLimits

        runtime = PythonRuntime(
            limits=RuntimeLimits(
                fuel=10_000_000_000,
                replenish_fuel_interval=30,
            )
        )

        @runtime.rpc.expose
        def add(a: int, b: int) -> int:
            return int(a) + int(b)

        @runtime.rpc.expose("host_upper")
        def uppercase(text: str) -> str:
            return text.upper()
        """
    ).strip()


def tutorial_program() -> str:
    return textwrap.dedent(
        """
        import sys

        print("hello from WASI Python")
        print("2 + 5 =", add(a=2, b=5))
        print(host_upper("guest code can call host functions"), flush=True)
        print("stderr stays separate", file=sys.stderr)
        """
    ).strip()


def worker_program() -> str:
    return textwrap.dedent(
        """
        class Calculator:
            def scale(self, value, by=1):
                return add(value, by) * by

        calculator = Calculator()
        """
    ).strip()


def worker_call_example() -> str:
    return textwrap.dedent(
        """
        worker = await runtime.execute_worker(worker_program, timeout=30)
        try:
            for value in range(3):
                result = await worker.call(("calculator", "scale"), value + 5, by=3, timeout=5)
                print("worker result:", result)
                await asyncio.sleep(0.25)
        finally:
            await worker.close()
        """
    ).strip()


def section(title: str) -> None:
    typewrite(f"\n{BOLD}{CYAN}== {title} =={RESET}")


def paragraph(text: str) -> None:
    wrapped = textwrap.fill(text, width=88)
    typewrite(wrapped)
    presentation_pause(pause_for_text(wrapped))


def pause_for_text(text: str) -> float:
    visible = strip_ansi(text)
    length = len(visible.replace("\n", " "))
    return min(
        PRESENTATION_PAUSE,
        max(PRESENTATION_MIN_PAUSE, length / PRESENTATION_READING_CHARS_PER_SECOND),
    )


def strip_ansi(text: str) -> str:
    stripped: list[str] = []
    index = 0

    while index < len(text):
        if text[index] == "\x1b":
            while index < len(text) and text[index] != "m":
                index += 1
            index += 1
            continue

        stripped.append(text[index])
        index += 1

    return "".join(stripped)


def code_block(source: str) -> None:
    for line in numbered_code_lines(source):
        typewrite(line)


def step(label: str) -> None:
    typewrite(f"{YELLOW}{label}{RESET}")


def prompt(text: str) -> str | None:
    try:
        return input(text)
    except (EOFError, KeyboardInterrupt):
        print()
        return None


def run_prompt() -> bool:
    return prompt(f"{YELLOW}try it{DIM} - press Enter to run...{RESET}") is not None


def continue_prompt() -> bool:
    return prompt(f"{DIM}continue...{RESET}") is not None


def execute_code(runtime: PythonRuntime, source: str) -> None:
    result = runtime.execute(source, timeout=30)

    typewrite(f"exit code: {result.exit_code}")
    typewrite(
        result.formatted_text(
            stderr=(f"{RED}[stderr] ".encode(), RESET.encode()),
        ),
        end="",
    )
    if result.text and not result.text.endswith("\n"):
        typewrite("")


def one_shot_demo(runtime: PythonRuntime) -> None:
    section("one-shot execution")
    paragraph("Guest code is sent on stdin to WASI CPython. The runtime prepends an API import, so exposed host RPC functions are available as ordinary callables.")
    step("demo")
    code_block(tutorial_program())
    if not run_prompt():
        return
    execute_code(runtime, tutorial_program())
    continue_prompt()


async def worker_demo(runtime: PythonRuntime) -> None:
    section("long-lived worker")
    paragraph("A worker keeps guest state alive. The host can call functions or methods inside that worker through the same RPC channel.")
    step("demo")
    code_block(worker_program())
    code_block(worker_call_example())
    if not run_prompt():
        return

    worker = await runtime.execute_worker(
        worker_program(),
        timeout=30,
    )
    try:
        for value in range(3):
            result = await worker.call(
                ("calculator", "scale"),
                value + 5,
                by=3,
                timeout=5,
            )
            typewrite(f"{GREEN}host -> guest worker call{RESET}: {result}")
            await asyncio.sleep(0.25)
    finally:
        await worker.close()
    continue_prompt()


HOST_SNIPPETS: tuple[tuple[str, str, str], ...] = (
    (
        "Alt-1",
        "create a runtime",
        textwrap.dedent(
            """
            runtime = PythonRuntime(
                limits=RuntimeLimits(
                    fuel=10_000_000_000,
                    replenish_fuel_interval=30,
                )
            )
            """
        ).strip(),
    ),
    (
        "Alt-2",
        "expose a host function",
        textwrap.dedent(
            """
            @runtime.rpc.expose
            def add(a: int, b: int) -> int:
                return a + b
            """
        ).strip(),
    ),
    (
        "Alt-3",
        "execute guest code",
        textwrap.dedent(
            """
            result = runtime.execute(\"\"\"
            print("hello from guest")
            print("2 + 5 =", add(a=2, b=5))
            \"\"\", timeout=30)
            print(result.formatted_text())
            """
        ).strip(),
    ),
    (
        "Alt-4",
        "execute a worker",
        textwrap.dedent(
            """
            async def main() -> None:
                worker = await runtime.execute_worker("value = 41", timeout=30)
                try:
                    print(await worker.call(("eval",), "value + 1", timeout=5))
                finally:
                    await worker.close()

            asyncio.run(main())
            """
        ).strip(),
    ),
)


def playground() -> None:
    section("playground")
    paragraph("Write a host Python program from start to end. The program can create a runtime, execute one-shot guest code, start long-lived guest workers, or expose host functions for the guest to call.")
    typewrite(f"{DIM}No runtime is created yet. Use Alt-1 or write PythonRuntime(...) yourself.{RESET}")

    host_namespace: dict[str, Any] = {
        "asyncio": asyncio,
        "RuntimeLimits": RuntimeLimits,
        "PythonRuntime": PythonRuntime,
    }

    while True:
        typewrite("")
        print_snippets()
        lines = read_repl_block()
        if lines is None:
            return

        if not lines:
            continue

        source = "\n".join(lines)
        print_code_block(source)
        print(f"{BOLD}{YELLOW}executing code...{RESET}")
        execute_host_code(source, host_namespace)
        if not continue_prompt():
            return


def print_snippets() -> None:
    typewrite(f"{DIM}Snippets:{RESET}")
    for key, label, source in HOST_SNIPPETS:
        typewrite(f"{DIM}- {key}: {label}{RESET}")
    typewrite(f"{DIM}{'-' * 72}{RESET}")


def print_code_block(source: str) -> None:
    for line in numbered_code_lines(source):
        print(line)


def read_repl_block() -> list[str] | None:
    if not sys.stdin.isatty():
        return read_line_repl_block()

    editor = TerminalBlockEditor()

    if sys.platform == "win32":
        try:
            import msvcrt
        except ImportError:
            return read_line_repl_block()

        return editor.read(WindowsKeyReader(msvcrt))

    try:
        import termios
        import tty
    except ImportError:
        return read_line_repl_block()

    return editor.read(PosixKeyReader(termios, tty))


def read_line_repl_block() -> list[str] | None:
    lines: list[str] = []

    while True:
        line = prompt("host>>> " if not lines else "... ")
        if line is None:
            return None

        if line == ":quit":
            return None

        if line == ":suggest" and not lines:
            return [line]

        if not line:
            return lines

        lines.append(line)


class KeyReader(Protocol):
    def __enter__(self) -> "KeyReader":
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        raise NotImplementedError

    def read_key(self) -> str:
        raise NotImplementedError


class PosixKeyReader:
    def __init__(self, termios: ModuleType, tty: ModuleType) -> None:
        self.termios = termios
        self.tty = tty
        self.fd = sys.stdin.fileno()
        self.old_settings: list[object] | None = None

    def __enter__(self) -> "PosixKeyReader":
        self.old_settings = self.termios.tcgetattr(self.fd)
        self.tty.setraw(self.fd)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        if self.old_settings is not None:
            self.termios.tcsetattr(
                self.fd,
                self.termios.TCSADRAIN,
                self.old_settings,
            )

    def read_key(self) -> str:
        key = os.read(self.fd, 1).decode(errors="ignore")
        if key != "\x1b":
            return key

        import select

        while select.select([self.fd], [], [], 0.01)[0]:
            key += os.read(self.fd, 1).decode(errors="ignore")
            if key.endswith("~") or key[-1:].isalpha():
                break

        return key


class WindowsKeyReader:
    def __init__(self, msvcrt: ModuleType) -> None:
        self.msvcrt = msvcrt

    def __enter__(self) -> "WindowsKeyReader":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        return

    def read_key(self) -> str:
        key = self.msvcrt.getwch()
        if key in {"\x00", "\xe0"}:
            return windows_extended_key(self.msvcrt.getwch())

        return key


def windows_extended_key(code: str) -> str:
    if code == "H":
        return "\x1b[A"
    if code == "P":
        return "\x1b[B"
    if code == "K":
        return "\x1b[D"
    if code == "M":
        return "\x1b[C"
    if code == "S":
        return "\x1b[3~"

    return ""


class TerminalBlockEditor:
    def __init__(self) -> None:
        self.lines = [""]
        self.cursor_line = 0
        self.cursor_column = 0
        self.viewport_top = 0
        self.viewport_left = 0
        self.rendered_lines = 0
        self.cursor_render_row = 0
        self.preferred_column: int | None = None

    @property
    def source(self) -> str:
        return "\n".join(self.lines)

    @source.setter
    def source(self, value: str) -> None:
        self.lines = value.split("\n") or [""]
        self.cursor_line = min(self.cursor_line, len(self.lines) - 1)
        self.cursor_column = min(
            self.cursor_column,
            len(self.lines[self.cursor_line]),
        )
        self.ensure_cursor_visible()

    def read(self, key_reader: KeyReader) -> list[str] | None:
        with key_reader:
            self.redraw()
            while True:
                key = key_reader.read_key()
                if key in {"\x03", "\x04"}:
                    return None

                if key == "\x12":
                    if not self.source.strip():
                        continue

                    self.clear_render()

                    return self.lines

                self.handle_key(key)
                self.redraw()

    def handle_key(self, key: str) -> None:
        if key in {"\r", "\n"}:
            self.insert("\n")
        elif key in {"\x7f", "\b"}:
            self.backspace()
        elif key == "\x01":
            self.cursor_column = 0
        elif key == "\x05":
            self.cursor_column = len(self.lines[self.cursor_line])
        elif key == "\x0c":
            self.lines = [""]
            self.cursor_line = 0
            self.cursor_column = 0
            self.viewport_top = 0
            self.viewport_left = 0
            self.preferred_column = None
        elif key == "\x1b[D":
            self.move_left()
        elif key == "\x1b[C":
            self.move_right()
        elif key == "\x1b[A":
            self.move_up()
        elif key == "\x1b[B":
            self.move_down()
        elif key == "\x1b[3~":
            self.delete()
        elif key in {"\x1b1", "\x1b2", "\x1b3", "\x1b4"}:
            self.insert_snippet(key[-1])
        elif key.isprintable() or key == "\t":
            self.insert(key)

    def insert_snippet(self, number: str) -> None:
        index = int(number) - 1
        if index < 0 or index >= len(HOST_SNIPPETS):
            return

        snippet = HOST_SNIPPETS[index][2]
        if self.source.strip():
            self.insert("\n" + snippet)
        else:
            self.source = snippet
            self.cursor_line = len(self.lines) - 1
            self.cursor_column = len(self.lines[self.cursor_line])
            self.preferred_column = None

    def insert(self, text: str) -> None:
        line = self.lines[self.cursor_line]
        before = line[: self.cursor_column]
        after = line[self.cursor_column :]
        parts = text.split("\n")

        if len(parts) == 1:
            self.lines[self.cursor_line] = before + text + after
            self.cursor_column += len(text)
        else:
            replacement = [before + parts[0], *parts[1:-1], parts[-1] + after]
            self.lines[self.cursor_line : self.cursor_line + 1] = replacement
            self.cursor_line += len(parts) - 1
            self.cursor_column = len(parts[-1])

        self.preferred_column = None

    def backspace(self) -> None:
        if self.cursor_column > 0:
            line = self.lines[self.cursor_line]
            self.lines[self.cursor_line] = (
                line[: self.cursor_column - 1] + line[self.cursor_column :]
            )
            self.cursor_column -= 1
            self.preferred_column = None
            return

        if self.cursor_line <= 0:
            return

        previous = self.lines[self.cursor_line - 1]
        current = self.lines.pop(self.cursor_line)
        self.cursor_line -= 1
        self.cursor_column = len(previous)
        self.lines[self.cursor_line] = previous + current
        self.preferred_column = None

    def delete(self) -> None:
        line = self.lines[self.cursor_line]
        if self.cursor_column < len(line):
            self.lines[self.cursor_line] = (
                line[: self.cursor_column] + line[self.cursor_column + 1 :]
            )
            self.preferred_column = None
            return

        if self.cursor_line >= len(self.lines) - 1:
            return

        self.lines[self.cursor_line] = line + self.lines.pop(self.cursor_line + 1)
        self.preferred_column = None

    def move_left(self) -> None:
        if self.cursor_column > 0:
            self.cursor_column -= 1
        elif self.cursor_line > 0:
            self.cursor_line -= 1
            self.cursor_column = len(self.lines[self.cursor_line])
        self.preferred_column = None

    def move_right(self) -> None:
        if self.cursor_column < len(self.lines[self.cursor_line]):
            self.cursor_column += 1
        elif self.cursor_line < len(self.lines) - 1:
            self.cursor_line += 1
            self.cursor_column = 0
        self.preferred_column = None

    def move_up(self) -> None:
        self.move_vertical(-1)

    def move_down(self) -> None:
        self.move_vertical(1)

    def move_vertical(self, direction: int) -> None:
        target_line = self.cursor_line + direction
        if target_line < 0 or target_line >= len(self.lines):
            return

        if self.preferred_column is None:
            self.preferred_column = self.cursor_column

        self.cursor_line = target_line
        self.cursor_column = min(
            self.preferred_column,
            len(self.lines[self.cursor_line]),
        )

    def redraw(self) -> None:
        self.clear_render()
        self.ensure_cursor_visible()
        text = self.render()
        sys.stdout.write(text)
        sys.stdout.flush()
        self.rendered_lines = len(text.splitlines())
        self.move_terminal_cursor_to_editor_cursor()

    def clear_render(self) -> None:
        if self.rendered_lines <= 0:
            return

        sys.stdout.write("\r")
        if self.cursor_render_row > 0:
            sys.stdout.write(f"\x1b[{self.cursor_render_row}A")
        sys.stdout.write("\x1b[J")
        sys.stdout.flush()
        self.rendered_lines = 0
        self.cursor_render_row = 0

    def move_terminal_cursor_to_editor_cursor(self) -> None:
        row, column = self.cursor_screen_position()
        up = self.rendered_lines - row
        sys.stdout.write("\r")
        if up > 0:
            sys.stdout.write(f"\x1b[{up}A")
        if column > 0:
            sys.stdout.write(f"\x1b[{column}C")
        sys.stdout.flush()
        self.cursor_render_row = row

    def cursor_screen_position(self) -> tuple[int, int]:
        line_count = max(1, len(self.lines))
        width = max(2, len(str(line_count)))
        before_marker_rows = 1 if self.viewport_top > 0 else 0
        row = before_marker_rows + (self.cursor_line - self.viewport_top)
        left_marker_columns = 2 if self.viewport_left > 0 else 0
        column = width + 3 + left_marker_columns + (
            self.cursor_column - self.viewport_left
        )
        return row, column

    def viewport_height(self) -> int:
        terminal_lines = shutil.get_terminal_size((80, 24)).lines
        return max(3, terminal_lines - 8)

    def viewport_width(self) -> int:
        terminal_columns = shutil.get_terminal_size((80, 24)).columns
        line_number_width = max(2, len(str(max(1, len(self.lines)))))
        prefix_columns = line_number_width + 3
        marker_columns = 4
        return max(8, terminal_columns - prefix_columns - marker_columns)

    def ensure_cursor_visible(self) -> None:
        height = self.viewport_height()
        width = self.viewport_width()

        if self.cursor_line < self.viewport_top:
            self.viewport_top = self.cursor_line
        elif self.cursor_line >= self.viewport_top + height:
            self.viewport_top = self.cursor_line - height + 1

        max_top = max(0, len(self.lines) - height)
        self.viewport_top = min(max(0, self.viewport_top), max_top)

        if self.cursor_column < self.viewport_left:
            self.viewport_left = self.cursor_column
        elif self.cursor_column >= self.viewport_left + width:
            self.viewport_left = self.cursor_column - width + 1

        max_line_length = max((len(line) for line in self.lines), default=0)
        max_left = max(0, max_line_length - width)
        self.viewport_left = min(max(0, self.viewport_left), max_left)

    def visible_line_range(self) -> tuple[int, int]:
        self.ensure_cursor_visible()
        start = self.viewport_top
        end = min(len(self.lines), start + self.viewport_height())
        return start, end

    def render(self) -> str:
        start, end = self.visible_line_range()
        visible_lines = self.lines[start:end]
        lines_above = start
        lines_below = len(self.lines) - end
        lines: list[str] = []
        if lines_above:
            lines.append(f"{DIM}... {lines_above} line(s) above ...{RESET}")

        lines.extend(
            numbered_code_lines_for_lines(
                visible_lines,
                start_index=start + 1,
                total_lines=len(self.lines),
                column_start=self.viewport_left,
                column_width=self.viewport_width(),
            )
        )

        if lines_below:
            lines.append(f"{DIM}... {lines_below} line(s) below ...{RESET}")

        lines.append(
            f"{DIM}Enter=new line  Arrows=move  Alt-1/2/3/4=snippets  Ctrl-R=run  Ctrl-L=clear  Ctrl-D=quit{RESET}"
        )
        return "\r\n".join(lines) + "\r\n"


def numbered_code_lines(source: str) -> list[str]:
    lines = source.split("\n") or [""]
    return numbered_code_lines_for_lines(
        lines,
        start_index=1,
        total_lines=len(lines),
        column_start=0,
        column_width=None,
    )


def numbered_code_lines_for_lines(
    lines: list[str],
    *,
    start_index: int,
    total_lines: int,
    column_start: int,
    column_width: int | None,
) -> list[str]:
    width = max(2, len(str(total_lines)))
    numbered: list[str] = []

    for index, line in enumerate(lines, start=start_index):
        visible = line
        left_marker = ""
        right_marker = ""

        if column_width is not None:
            end = column_start + column_width
            visible = line[column_start:end]

            if column_start > 0:
                left_marker = f"{DIM}< {RESET}"
            if end < len(line):
                right_marker = f"{DIM} >{RESET}"

        numbered.append(
            f"{DIM}{index:>{width}} |{RESET} "
            f"{left_marker}{highlight_python(visible)}{right_marker}"
        )

    return numbered


def enable_readline() -> None:
    try:
        import readline
    except ImportError:
        return

    readline.parse_and_bind("set editing-mode emacs")


def print_host_result(result: object) -> None:
    if result is None:
        return

    if isinstance(result, str):
        typewrite(result, end="" if result.endswith("\n") else "\n")
        return

    typewrite(repr(result))


def execute_host_code(source: str, namespace: dict[str, Any]) -> None:
    try:
        if execute_host_expression(source, namespace):
            return

        execute_host_statements(source, namespace)
    except BaseException:
        typewrite(f"{RED}{traceback.format_exc()}{RESET}", end="")


def execute_host_expression(source: str, namespace: dict[str, Any]) -> bool:
    try:
        expression = compile(source, "<host>", "eval")
    except SyntaxError:
        return False

    result = eval(expression, namespace)
    print_host_result(result)
    return True


def execute_host_statements(source: str, namespace: dict[str, Any]) -> None:
    exec(compile(source, "<host>", "exec"), namespace)


def highlight_python(source: str) -> str:
    lines = source.splitlines(keepends=True)
    highlighted: list[str] = []
    row = 1
    column = 0

    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)

        for token in tokens:
            if token.type in {
                tokenize.ENCODING,
                tokenize.ENDMARKER,
                tokenize.INDENT,
                tokenize.DEDENT,
            }:
                continue

            start_row, start_column = token.start
            end_row, end_column = token.end

            while row < start_row:
                highlighted.append(lines[row - 1][column:])
                row += 1
                column = 0

            if row == start_row:
                highlighted.append(lines[row - 1][column:start_column])

            highlighted.append(highlight_token(token))
            row = end_row
            column = end_column
    except (IndentationError, tokenize.TokenError):
        pass

    if lines and row <= len(lines):
        highlighted.append(lines[row - 1][column:])
        highlighted.extend(lines[row:])

    return "".join(highlighted) if highlighted else source


def highlight_token(token: tokenize.TokenInfo) -> str:
    text = token.string
    if token.type == tokenize.NAME:
        if keyword.iskeyword(text):
            return f"{MAGENTA}{text}{RESET}"
        if text in dir(builtins):
            return f"{CYAN}{text}{RESET}"
        return text

    if token.type == tokenize.STRING:
        return f"{GREEN}{text}{RESET}"

    if token.type == tokenize.NUMBER:
        return f"{YELLOW}{text}{RESET}"

    if token.type == tokenize.COMMENT:
        return f"{DIM}{text}{RESET}"

    if token.type == tokenize.OP:
        return f"{WHITE}{text}{RESET}"

    return text


def main() -> None:
    args = parse_args()
    enable_readline()

    if args.editor:
        playground()
        return

    runtime = create_runtime()

    typewrite(f"{BOLD}pysandbox tutorial{RESET}")
    paragraph("This demo runs WASI CPython in a spawned worker process, exposes host functions over RPC, captures interlaced stdout/stderr, and then starts a long-lived guest worker.")

    section("host setup")
    paragraph("Create a runtime and expose trusted host functions. The guest can call these by name.")
    step("demo")
    code_block(host_setup_example())
    if not run_prompt():
        return
    typewrite(f"{GREEN}runtime ready{RESET}: exposed methods = {', '.join(runtime.rpc.methods)}")
    if not continue_prompt():
        return

    one_shot_demo(runtime)
    asyncio.run(worker_demo(runtime))
    playground()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        typewrite("")
