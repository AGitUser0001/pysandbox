import asyncio
import os
import re
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from multiprocessing.context import SpawnProcess
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from ..messaging.messanger import Messanger
from .assets import Asset
from .rpc_host import RpcHost
from .runtime import (
    Runtime,
    RuntimeError,
    RuntimeExecutionError,
    RuntimeLimits,
    RuntimeMount,
    RuntimeParameters,
    RuntimeResult,
    WasmtimeEnvironment,
)


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
    messanger_ready: threading.Condition
    messanger: Messanger | None = None

    def set_messanger(self, messanger: Messanger) -> None:
        with self.messanger_ready:
            self.messanger = messanger
            self.messanger_ready.notify_all()

    def wait_messanger(self, timeout: float | None = None) -> Messanger:
        with self.messanger_ready:
            if self.messanger is None:
                self.messanger_ready.wait(timeout)

            if self.messanger is None:
                raise TimeoutError("worker messenger was not ready")

            return self.messanger


@dataclass
class Worker:
    runtime: "PythonRuntime"
    parameters: "PythonRuntimeParameters"
    process: SpawnProcess
    connection: Connection
    execution: PythonRpcExecution
    task: asyncio.Task[RuntimeResult]

    async def call(
        self,
        fn_path: tuple[str, ...],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Any:
        messanger = await asyncio.to_thread(self.execution.wait_messanger, timeout)
        return await asyncio.to_thread(
            self.runtime.rpc.call_worker,
            messanger,
            fn_path,
            args,
            kwargs,
            timeout=timeout,
        )

    def terminate(self) -> None:
        self.process.terminate()

    async def close(self) -> None:
        self.terminate()
        try:
            await self.task
        except (EOFError, RuntimeError):
            return


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
        root: Path | None = None,
        python_version: str | None = None,
        limits: RuntimeLimits | None = None,
        api: bool = True,
    ) -> None:
        super().__init__(limits=limits)
        self.root = root or PACKAGE_ROOT / "python-wasi"
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

    def generate_runtime_parameters(self, program: str | bytes) -> RuntimeParameters:
        install = self.ensure_runtime()
        stdin = self.prepare_program(program)
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
            env.update(
                {
                    "PYSANDBOX_RPC_DIR": message_pipe.guest_dir,
                    "PYSANDBOX_RPC_METHODS": ",".join(self.rpc_methods()),
                }
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
            argv=("python.wasm", "-I", "-B"),
            env=env,
            stdin=stdin,
            mounts=mounts,
            message_pipe=message_pipe,
        )

    def prepare_program(self, program: str | bytes) -> bytes:
        source = program.encode("utf-8") if isinstance(program, str) else program
        prefix: list[bytes] = []

        if self.limits.replenish_fuel_interval is not None:
            prefix.append(
                (
                    "import os, sys, time\n"
                    "def __pysandbox_meter(_, __, ___, *, os=os, time=time, "
                    f"state=[time.monotonic() + {self.limits.replenish_fuel_interval!r}, None]):\n"
                    "    now = time.monotonic()\n"
                    "    if now >= state[0]:\n"
                    "        os.sched_yield()\n"
                    f"        state[0] = now + {self.limits.replenish_fuel_interval!r}\n"
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

    async def execute_worker(
        self,
        program: str | bytes,
        *,
        timeout: float | None = None,
    ) -> Worker:
        if not self.api:
            raise RuntimeExecutionError("Python worker mode requires api=True")

        parameters = self.generate_runtime_parameters(self.worker_program(program))
        if (
            not isinstance(parameters, PythonRuntimeParameters)
            or parameters.message_pipe is None
        ):
            raise RuntimeExecutionError("Python worker mode requires a message pipe")

        parent_connection, child_connection = self.create_worker_pipe()
        process = self.create_worker_process(
            parameters,
            child_connection,
            timeout=timeout,
        )
        self.before_worker_start(parameters)
        try:
            with self.suppress_main_module_fixup():
                process.start()
        except BaseException:
            parent_connection.close()
            child_connection.close()
            self.after_worker_finish(parameters)
            raise

        child_connection.close()
        execution = self.rpc_execution_for(parameters)
        if execution is None:
            parent_connection.close()
            self.after_worker_finish(parameters)
            raise RuntimeExecutionError("Python worker RPC execution was not started")

        task = asyncio.create_task(
            self.finish_worker_process(
                parent_connection,
                parameters,
                process,
                timeout=timeout,
            )
        )
        return Worker(
            runtime=self,
            parameters=parameters,
            process=process,
            connection=parent_connection,
            execution=execution,
            task=task,
        )

    def worker_program(self, program: str | bytes) -> bytes:
        source = program.encode("utf-8") if isinstance(program, str) else program
        return source + b"\nspin()\n"

    async def finish_worker_process(
        self,
        connection: Connection,
        parameters: PythonRuntimeParameters,
        process: SpawnProcess,
        *,
        timeout: float | None,
    ) -> RuntimeResult:
        return await asyncio.to_thread(
            self.finish_worker_process_sync,
            connection,
            parameters,
            process,
            timeout=timeout,
        )

    def finish_worker_process_sync(
        self,
        connection: Connection,
        parameters: PythonRuntimeParameters,
        process: SpawnProcess,
        *,
        timeout: float | None,
    ) -> RuntimeResult:
        try:
            try:
                response = self.recv_worker_response(
                    connection,
                    parameters,
                    process,
                    timeout=timeout,
                )
            finally:
                connection.close()

            process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join()

            if isinstance(response, RuntimeResult):
                return response

            raise RuntimeExecutionError(
                f"{response.error_type}: {response.message}\n{response.traceback}"
            )
        finally:
            self.after_worker_finish(parameters)

    def before_execution(
        self,
        environment: WasmtimeEnvironment,
        parameters: RuntimeParameters,
    ) -> None:
        return

    def before_worker_start(self, parameters: RuntimeParameters) -> None:
        if not isinstance(parameters, PythonRuntimeParameters):
            return

        if parameters.message_pipe is None:
            return

        stop = threading.Event()
        violation = threading.Event()
        execution = PythonRpcExecution(
            thread=threading.Thread(),
            stop=stop,
            violation=violation,
            messanger_ready=threading.Condition(threading.RLock()),
        )
        thread = threading.Thread(
            target=self.rpc.dispatch_file_forever,
            args=(
                parameters.message_pipe.request_files,
                parameters.message_pipe.response_files,
                stop,
            ),
            kwargs={
                "max_request_bytes": self.limits.max_rpc_message_bytes,
                "on_oversized_request": violation.set,
                "on_messanger_ready": execution.set_messanger,
            },
            name="python-rpc-host",
            daemon=True,
        )
        execution.thread = thread
        with self._active_rpc_lock:
            self._active_rpc_executions[parameters.message_pipe.host_dir] = execution

        thread.start()

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

    def after_worker_finish(self, parameters: RuntimeParameters) -> None:
        if not isinstance(parameters, PythonRuntimeParameters):
            return

        execution: PythonRpcExecution | None = None
        if parameters.message_pipe is not None:
            with self._active_rpc_lock:
                execution = self._active_rpc_executions.pop(
                    parameters.message_pipe.host_dir,
                    None,
                )

        if execution is not None:
            execution.stop.set()
            execution.thread.join(timeout=1)

        if parameters.message_pipe is not None:
            parameters.message_pipe.cleanup()

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
        return self.rpc.methods

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
        existing = self.inspect_runtime()
        if existing is None:
            self.runtime_asset.fetch(self.runtime_root)

            existing = self.inspect_runtime()
            if existing is None:
                raise RuntimeError(
                    f"downloaded CPython WASI runtime is invalid at {self.runtime_root}"
                )

        self.ensure_guest_packages(existing)

        return existing

    def ensure_guest_packages(self, install: PythonWasiInstall) -> None:
        site_packages = self.site_packages(install)
        if not (site_packages / "cbor2" / "__init__.py").is_file():
            source_root = site_packages / ".cbor2-source"
            self.cbor2_asset.fetch(source_root)
            self.install_cbor2_package(source_root, site_packages / "cbor2")

        self.copy_guest_messaging(site_packages)

    @staticmethod
    def site_packages(install: PythonWasiInstall) -> Path:
        return (
            install.runtime_root
            / "lib"
            / f"python{install.python_version}"
            / "site-packages"
        )

    def install_cbor2_package(self, source_root: Path, destination: Path) -> None:
        source = source_root / "cbor2"
        if not (source / "__init__.py").is_file():
            shutil.rmtree(source_root)
            raise RuntimeError("downloaded cbor2 source does not contain /cbor2")

        if destination.exists():
            shutil.rmtree(destination)

        shutil.copytree(source, destination)
        shutil.rmtree(source_root)

    def copy_guest_messaging(self, site_packages: Path) -> None:
        site_packages.mkdir(parents=True, exist_ok=True)
        messaging_package = site_packages / "messaging"
        messaging_package.mkdir(parents=True, exist_ok=True)

        (messaging_package / "__init__.py").write_text("", encoding="utf-8")
        shutil.copyfile(
            Path(__file__).parents[1] / "messaging" / "messanger.py",
            messaging_package / "messanger.py",
        )
        shutil.copyfile(
            Path(__file__).parents[1] / "messaging" / "transports.py",
            messaging_package / "transports.py",
        )
        shutil.copyfile(
            Path(__file__).parents[1] / "messaging" / "api.py",
            messaging_package / "api.py",
        )
        (site_packages / "api.py").write_text(
            "from messaging.api import *\n",
            encoding="utf-8",
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
