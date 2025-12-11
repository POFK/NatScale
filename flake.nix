{
  description = "NATS Dev Environment for HPC Inference";

  # 使用 unstable 分支以获取最新的 NATS 版本 (这对 JetStream 功能很重要)
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          # 这里列出你需要的包
          packages = [
            pkgs.nats-server # 服务端
            pkgs.natscli     # 客户端工具 (命令是 'nats')
            
            # 如果你也需要 Python 环境，可以在这里解开注释
            # pkgs.python3
          ];

          # 进入环境时自动运行的 Shell 脚本/提示
          shellHook = ''
            echo "================================================"
            echo "Welcome to the NATS Development Environment!"
            echo "------------------------------------------------"
            echo "Tools installed:"
            echo "  - $(nats-server --version)"
            echo "  - $(nats --version)"
            echo ""
            echo "Quick Start Commands:"
            echo "  1. Start Server (JetStream):  start-server"
            echo "  2. Client Tool:               nats"
            echo "================================================"

            # 定义一个简单的别名函数来快速启动带 JetStream 的服务器
            alias start-server="nats-server -js -m 8222"
          '';
        };
      }
    );
}
