import asyncio
import os
import re
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass, field
from multiprocessing.context import SpawnProcess
from pathlib import Path
from typing import Any

from ..messaging import Messenger
from .assets import Asset
from .rpc_host import RpcHost
from .runtime import (
    decode_output_event,
    Runtime,
    RuntimeError,
    RuntimeExecutionError,
    RuntimeLimits,
    RuntimeMount,
    RuntimeParameters,
    RuntimeResult,
    RuntimeStartCallback,
    RuntimeWorkerStartCallback,
    RuntimeSetupError,
    WasmtimeEnvironment,
    Worker,
)


__all__ = [
    "PythonMessagePipe",
    "PythonRuntime",
    "PythonRuntimeParameters",
    "PythonWorker",
    "PythonWasiInstall",
]


PACKAGE_ROOT = Path(__file__).parents[1]


@dataclass(frozen=True)
class PythonWasiInstall:
    runtime_root: Path
    python_wasm: Path
    python_version: str


@dataclass(frozen=True)
class PythonMessagePipe:
    host_dir: Path
    guest_dir: str = "/__pysandbox_rpc__"
    request_name: str = "request"
    response_name: str = "response"

    @property
    def request_dir(self) -> Path:
        return self.host_dir / self.request_name

    @property
    def request_files(self) -> tuple[Path, Path]:
        return (
            self.request_dir / "0",
            self.request_dir / "1",
        )

    @property
    def response_dir(self) -> Path:
        return self.host_dir / self.response_name

    @property
    def response_files(self) -> tuple[Path, Path]:
        return (
            self.response_dir / "0",
            self.response_dir / "1",
        )

    @classmethod
    def create(cls) -> "PythonMessagePipe":
        host_dir = Path(tempfile.mkdtemp(prefix="pysandbox-python-rpc-"))
        pipe = cls(host_dir=host_dir)

        try:
            pipe.request_dir.mkdir()
            pipe.response_dir.mkdir()
            for path in (*pipe.request_files, *pipe.response_files):
                path.write_bytes(b"")

            pipe.lock_permissions()
        except Exception:
            pipe.cleanup()
            raise

        return pipe

    def cleanup(self) -> None:
        self.unlock_permissions()
        shutil.rmtree(self.host_dir, ignore_errors=True)

    def lock_permissions(self) -> None:
        if os.name == "nt":
            self.lock_permissions_windows()
        else:
            self.lock_permissions_posix()

    def unlock_permissions(self) -> None:
        if os.name == "nt":
            self.unlock_permissions_windows()
        else:
            self.unlock_permissions_posix()

    def lock_permissions_posix(self) -> None:
        self.request_dir.chmod(0o500)
        self.response_dir.chmod(0o500)
        self.host_dir.chmod(0o500)

    def unlock_permissions_posix(self) -> None:
        for path in (self.request_dir, self.response_dir, self.host_dir):
            try:
                path.chmod(0o700)
            except OSError:
                pass

    def lock_permissions_windows(self) -> None:
        user = windows_acl_identity()
        for directory in (self.host_dir, self.request_dir, self.response_dir):
            icacls(directory, "/inheritance:r", "/grant:r", f"{user}:RX")

        for path in (*self.request_files, *self.response_files):
            icacls(path, "/inheritance:r", "/grant:r", f"{user}:F")

    def unlock_permissions_windows(self) -> None:
        user = windows_acl_identity()
        for path in (
            *self.request_files,
            *self.response_files,
            self.request_dir,
            self.response_dir,
            self.host_dir,
        ):
            try:
                icacls(path, "/grant:r", f"{user}:F")
            except (FileNotFoundError, subprocess.CalledProcessError):
                pass


@dataclass
class PythonRuntimeParameters(RuntimeParameters):
    message_pipe: PythonMessagePipe | None = None


@dataclass
class PythonRpcExecution:
    thread: threading.Thread
    stop: threading.Event
    violation: threading.Event
    messenger_ready: threading.Condition
    limits: RuntimeLimits
    limits_lock: threading.RLock = field(default_factory=threading.RLock)
    messenger: Messenger | None = None

    def set_messenger(self, messenger: Messenger) -> None:
        with self.messenger_ready:
            self.messenger = messenger
            self.messenger_ready.notify_all()

    def wait_messenger(self, timeout: float | None = None) -> Messenger:
        with self.messenger_ready:
            if self.messenger is None:
                self.messenger_ready.wait(timeout)

            if self.messenger is None:
                raise TimeoutError("worker messenger was not ready")

            return self.messenger

    def update_limits(self, limits: RuntimeLimits) -> None:
        with self.limits_lock:
            self.limits = limits

    def max_request_bytes(self) -> int:
        with self.limits_lock:
            return self.limits.max_rpc_message_bytes

    def max_file_bytes(self) -> int:
        with self.limits_lock:
            return self.limits.max_rpc_file_bytes


@dataclass
class PythonWorker(Worker):
    async def call(
        self,
        fn_path: tuple[str, ...],
        *args: object,
        timeout: float | None = None,
        **kwargs: object,
    ) -> object:
        if not isinstance(self.runtime, PythonRuntime):
            raise RuntimeExecutionError("Python worker has an invalid runtime")

        execution = await self.execution_future
        if not isinstance(execution, PythonRpcExecution):
            raise RuntimeExecutionError("Python worker RPC execution was not started")

        messenger = await asyncio.to_thread(execution.wait_messenger, timeout)
        return await asyncio.to_thread(
            self.runtime.rpc.call_worker,
            messenger,
            fn_path,
            args,
            kwargs,
            timeout=timeout,
        )


def windows_acl_identity() -> str:
    username = os.environ.get("USERNAME")
    domain = os.environ.get("USERDOMAIN")

    if username and domain:
        return f"{domain}\\{username}"

    if username:
        return username

    user = os.environ.get("USER")
    if user:
        return user

    return "Users"


def icacls(path: Path, *args: str) -> None:
    if not path.exists() and not path.is_symlink():
        return

    subprocess.run(
        ["icacls", str(path), *args, "/C"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )


class PythonRuntime(Runtime):
    def __init__(
        self,
        *,
        root: Path = PACKAGE_ROOT / "python-wasi",
        python_version: str | None = None,
        api: bool = True,
    ) -> None:
        super().__init__()
        self.root = root
        self.python_version = python_version
        self.api = api
        self.runtime_asset = Asset(
            repo="brettcannon/cpython-wasi-build",
            filename=self.python_asset_filename(),
            extract=True,
            user_agent="pysandbox-python-runtime",
            timeout=120,
        )
        self.cbor2_asset = Asset(
            repo="agronholm/cbor2",
            tag=re.compile(r"5\.\d+\.\d+"),
            source=True,
            extract=True,
            extract_subdir="cbor2",
            user_agent="pysandbox-python-runtime",
            timeout=120,
        )
        self.rpc = RpcHost()
        self._active_rpc_executions: dict[Path, PythonRpcExecution] = {}
        self._active_rpc_lock = threading.RLock()

    @property
    def name(self) -> str:
        return "python"

    @property
    def runtime_root(self) -> Path:
        return self.root / "runtime"

    @property
    def cbor2_root(self) -> Path:
        return self.root / "cbor2"

    def generate_runtime_parameters(
        self,
        program: str | bytes,
        limits: RuntimeLimits,
    ) -> RuntimeParameters:
        install = self.ensure_runtime()
        stdin = self.prepare_program(program, limits)
        message_pipe = PythonMessagePipe.create() if self.api else None
        env: dict[str, str] = {}
        mounts = [
            RuntimeMount(
                host=install.runtime_root,
                guest="/",
                readonly=True,
            ),
        ]

        if message_pipe is not None:
            guest_site_packages = (
                f"/lib/python{install.python_version}/site-packages"
            )
            env.update(
                {
                    "PYSANDBOX_RPC_DIR": message_pipe.guest_dir,
                    "PYSANDBOX_RPC_METHODS": ",".join(self.rpc_methods()),
                    "PYSANDBOX_RPC_FILE_BYTES": str(limits.max_rpc_file_bytes),
                }
            )
            mounts.append(
                RuntimeMount(
                    host=PACKAGE_ROOT / "messaging",
                    guest=f"{guest_site_packages}/messaging",
                    readonly=True,
                )
            )
            mounts.append(
                RuntimeMount(
                    host=PACKAGE_ROOT / "guest_api_shim",
                    guest=f"{guest_site_packages}/api",
                    readonly=True,
                )
            )
            mounts.append(
                RuntimeMount(
                    host=self.cbor2_root,
                    guest=f"{guest_site_packages}/cbor2",
                    readonly=True,
                )
            )
            mounts.append(
                RuntimeMount(
                    host=message_pipe.host_dir,
                    guest=message_pipe.guest_dir,
                    readonly=True,
                    file_readonly=True,
                )
            )
            mounts.append(
                RuntimeMount(
                    host=message_pipe.request_dir,
                    guest=f"{message_pipe.guest_dir}/{message_pipe.request_name}",
                    readonly=False,
                    file_readonly=False,
                )
            )
            mounts.append(
                RuntimeMount(
                    host=message_pipe.response_dir,
                    guest=f"{message_pipe.guest_dir}/{message_pipe.response_name}",
                    readonly=True,
                    file_readonly=True,
                )
            )

        return PythonRuntimeParameters(
            wasm_path=install.python_wasm,
            limits=limits,
            argv=("python.wasm", "-I"),
            env=env,
            stdin=stdin,
            mounts=mounts,
            message_pipe=message_pipe,
        )

    def prepare_program(self, program: str | bytes, limits: RuntimeLimits) -> bytes:
        source = program.encode("utf-8") if isinstance(program, str) else program
        prefix: list[bytes] = []

        if limits.replenish_fuel_interval is not None:
            prefix.append(
                (
                    "import os, sys, time\n"
                    "def __pysandbox_meter(_, __, ___, *, os=os, time=time, "
                    f"state=[time.monotonic() + {limits.replenish_fuel_interval!r}, None]):\n"
                    "    now = time.monotonic()\n"
                    "    if now >= state[0]:\n"
                    "        os.sched_yield()\n"
                    f"        state[0] = now + {limits.replenish_fuel_interval!r}\n"
                    "    return state[1]\n"
                    "__pysandbox_meter.__kwdefaults__['state'][1] = __pysandbox_meter\n"
                    "sys.settrace(__pysandbox_meter)\n"
                    "sys._getframe().f_trace = __pysandbox_meter\n"
                    "del os, sys, time, __pysandbox_meter\n"
                ).encode("utf-8")
            )

        if self.api:
            prefix.append(b"from api import *\n")

        return b"".join(prefix) + source

    def execute(
        self,
        program: str | bytes,
        *,
        limits: RuntimeLimits | None = None,
        timeout: float | None = None,
        after_start: RuntimeStartCallback | None = None,
        worker_after_start: RuntimeWorkerStartCallback | None = None,
        result: RuntimeResult | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        spin: bool = False,
    ) -> RuntimeResult:
        return super().execute(
            self.spin_program(program) if spin else program,
            limits=limits,
            timeout=timeout,
            after_start=after_start,
            worker_after_start=worker_after_start,
            result=result,
            loop=loop,
        )

    def run(
        self,
        program: str | bytes,
        *,
        limits: RuntimeLimits | None = None,
        timeout: float | None = None,
        after_start: RuntimeStartCallback | None = None,
        spin: bool = False,
    ) -> PythonWorker:
        worker = super().run(
            self.spin_program(program) if spin else program,
            limits=limits,
            timeout=timeout,
            after_start=after_start,
        )
        return PythonWorker(
            runtime=worker.runtime,
            task=worker.task,
            execution_future=worker.execution_future,
            result=worker.result,
            control_future=worker.control_future,
        )

    def spin_program(self, program: str | bytes) -> bytes:
        if not self.api:
            raise RuntimeExecutionError("Python spin mode requires api=True")

        source = program.encode("utf-8") if isinstance(program, str) else program
        return source + b"\n__import__('api').spin()\n"

    def rpc_execution_for(
        self,
        parameters: RuntimeParameters,
    ) -> PythonRpcExecution | None:
        if not isinstance(parameters, PythonRuntimeParameters):
            return None

        if parameters.message_pipe is None:
            return None

        with self._active_rpc_lock:
            return self._active_rpc_executions.get(parameters.message_pipe.host_dir)

    def rpc_methods(self) -> tuple[str, ...]:
        if not self.api:
            return ()

        return self.rpc.methods

    def before_execution(
        self,
        environment: WasmtimeEnvironment,
        parameters: RuntimeParameters,
    ) -> None:
        return

    def before_worker_start(
        self,
        parameters: RuntimeParameters,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> object | None:
        if not isinstance(parameters, PythonRuntimeParameters):
            return None

        if parameters.message_pipe is None:
            return None

        stop = threading.Event()
        violation = threading.Event()
        execution = PythonRpcExecution(
            thread=threading.Thread(),
            stop=stop,
            violation=violation,
            messenger_ready=threading.Condition(threading.RLock()),
            limits=parameters.limits,
        )
        thread = threading.Thread(
            target=self.rpc.dispatch_file_forever,
            args=(
                parameters.message_pipe.request_files,
                parameters.message_pipe.response_files,
                stop,
            ),
            kwargs={
                "max_request_bytes": execution.max_request_bytes,
                "max_file_bytes": execution.max_file_bytes,
                "on_oversized_request": violation.set,
                "on_messenger_ready": execution.set_messenger,
                "event_loop": self.rpc.event_loop or loop,
            },
            name="python-rpc-host",
            daemon=True,
        )
        execution.thread = thread
        with self._active_rpc_lock:
            self._active_rpc_executions[parameters.message_pipe.host_dir] = execution

        thread.start()
        return execution

    def check_worker_process(
        self,
        parameters: RuntimeParameters,
        process: SpawnProcess,
    ) -> None:
        execution = self.rpc_execution_for(parameters)
        if execution is None:
            return

        if not execution.violation.is_set():
            return

        terminate = getattr(process, "terminate", None)
        join = getattr(process, "join", None)
        is_alive = getattr(process, "is_alive", None)
        kill = getattr(process, "kill", None)

        if callable(terminate):
            terminate()

        if callable(join):
            join(timeout=1)

        if callable(is_alive) and is_alive() and callable(kill):
            kill()
            if callable(join):
                join()

        raise RuntimeExecutionError("runtime worker killed after oversized RPC request")

    def after_worker_finish(
        self,
        parameters: RuntimeParameters,
        *,
        execution: object | None = None,
    ) -> None:
        if not isinstance(parameters, PythonRuntimeParameters):
            return

        if parameters.message_pipe is not None:
            with self._active_rpc_lock:
                self._active_rpc_executions.pop(
                    parameters.message_pipe.host_dir,
                    None,
                )

        if isinstance(execution, PythonRpcExecution):
            execution.stop.set()
            if execution.messenger is not None:
                self.rpc.close_worker(execution.messenger)
            execution.thread.join(timeout=1)

        if parameters.message_pipe is not None:
            parameters.message_pipe.cleanup()

    def after_execution(
        self,
        environment: WasmtimeEnvironment,
        parameters: RuntimeParameters,
        *,
        result: RuntimeResult | None,
        error: BaseException | None,
    ) -> None:
        return

    def __getstate__(self) -> dict[str, object]:
        state = self.__dict__.copy()
        state["rpc"] = RpcHost()
        state["_active_rpc_executions"] = {}
        state["_active_rpc_lock"] = None
        return state

    def __setstate__(self, state: dict[str, object]) -> None:
        for key, value in state.items():
            setattr(self, key, value)
        self._active_rpc_lock = threading.RLock()

    def ensure_runtime(self) -> PythonWasiInstall:
        runtime_changed = self.runtime_asset.fetch(self.runtime_root)
        existing = self.inspect_runtime()
        if existing is None:
            raise RuntimeError(
                f"downloaded CPython WASI runtime is invalid at {self.runtime_root}"
            )

        if runtime_changed:
            self.compile_guest_python_files(existing, guest_tree="/lib")

        if self.api:
            if self.cbor2_asset.fetch(self.cbor2_root):
                guest_cbor2_path = (
                    f"/lib/python{existing.python_version}/site-packages/cbor2"
                )
                self.compile_guest_python_files(
                    existing,
                    guest_tree=guest_cbor2_path,
                    mounts=[
                        RuntimeMount(
                            host=self.cbor2_root,
                            guest=guest_cbor2_path,
                            readonly=False,
                            file_readonly=False,
                        ),
                    ],
                )

        return existing

    def compile_guest_python_files(
        self,
        install: PythonWasiInstall,
        *,
        guest_tree: str,
        mounts: list[RuntimeMount] | None = None,
    ) -> None:
        script = (
            "import compileall, sys\n"
            f"sys.exit(not compileall.compile_dir({guest_tree!r}, quiet=1))\n"
        )
        runtime_mount = RuntimeMount(
            host=install.runtime_root,
            guest="/",
            readonly=False,
            file_readonly=False,
        )
        parent_output_connection, child_output_connection = self.create_worker_pipe()
        child_control_connection, parent_control_connection = self.create_worker_pipe()
        try:
            result = self.execute_in_worker(
                RuntimeParameters(
                    wasm_path=install.python_wasm,
                    argv=("python.wasm", "-I"),
                    stdin=script.encode("utf-8"),
                    mounts=[runtime_mount, *(mounts or [])],
                ),
                child_output_connection,
                child_control_connection,
                timeout=30,
            )
        finally:
            child_output_connection.close()
            child_control_connection.close()
            parent_control_connection.close()

        try:
            while parent_output_connection.poll():
                try:
                    packet = parent_output_connection.recv_bytes()
                except EOFError:
                    break

                result.output.append(decode_output_event(packet))
        finally:
            parent_output_connection.close()

        if result.exit_code != 0:
            raise RuntimeSetupError(
                "failed to compile guest Python files\n"
                + result.formatted_text(
                    stderr=(b"[stderr] ", None),
                )
            )

    def python_asset_filename(self) -> re.Pattern[str]:
        if self.python_version is None:
            version = r"\d+\.\d+\.\d+"

        elif re.fullmatch(r"\d+\.\d+\.\d+", self.python_version):
            version = re.escape(self.python_version)

        elif re.fullmatch(r"\d+\.\d+", self.python_version):
            version = re.escape(self.python_version) + r"\.\d+"

        else:
            raise ValueError("python_version must look like '3.14' or '3.14.5'")

        return re.compile(rf"python-{version}-wasi_sdk-\d+\.zip")

    def inspect_runtime(self) -> PythonWasiInstall | None:
        if not self.runtime_root.is_dir():
            return None

        python_wasm = self.find_python_wasm(self.runtime_root)
        python_version = self.find_stdlib_version(self.runtime_root)
        if python_wasm is None or python_version is None:
            return None

        return PythonWasiInstall(
            runtime_root=self.runtime_root,
            python_wasm=python_wasm,
            python_version=python_version,
        )

    @staticmethod
    def find_python_wasm(root: Path) -> Path | None:
        candidates = (
            root / "python.wasm",
            root / "bin" / "python.wasm",
        )

        for candidate in candidates:
            if candidate.is_file():
                return candidate

        matches = sorted(root.rglob("python*.wasm"))
        return matches[0] if matches else None

    @staticmethod
    def find_stdlib_version(root: Path) -> str | None:
        lib = root / "lib"
        if not lib.is_dir():
            return None

        versions = sorted(
            path.name.removeprefix("python")
            for path in lib.iterdir()
            if path.is_dir() and re.fullmatch(r"python\d+\.\d+", path.name)
        )

        return versions[-1] if versions else None
