import os
import subprocess

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        if self.target_name != "wheel":
            return
        manifest = os.path.join(self.root, "nuefsd", "Cargo.toml")
        target_dir = os.path.join(self.root, "nuefsd", "target")
        subprocess.check_call(
            [
                "cargo",
                "build",
                "--release",
                "--manifest-path",
                manifest,
                "--target-dir",
                target_dir,
            ]
        )
        binary = os.path.join(target_dir, "release", "nuefsd")
        build_data["shared_data"][binary] = "bin/nuefsd"
        build_data["pure_python"] = False
        build_data["infer_tag"] = True
