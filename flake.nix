{
  description = "ztreamer — Zcash indexer and CompactTxStreamer";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      eachSystem = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      zakuraHash = "sha256-htoyDv4xAa1C4esketDu8dMEAoaeYyha3eoc2qqJ/c8=";
    in
    {
      packages = eachSystem (pkgs: {
        default = pkgs.rustPlatform.buildRustPackage {
          pname = "ztreamerd";
          version = "0.0.1";
          src = pkgs.lib.cleanSource ./.;
          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = pkgs.lib.genAttrs [
              "zakura-1.3.0"
              "zakura-chain-6.0.0"
              "zakura-consensus-7.0.0"
              "zakura-header-chain-1.0.0"
              "zakura-jsonl-trace-1.2.0"
              "zakura-network-7.0.0"
              "zakura-node-services-3.2.1"
              "zakura-rpc-8.0.0"
              "zakura-script-3.2.1"
              "zakura-state-7.0.0"
              "zakura-test-2.1.0"
              "zakura-tower-batch-control-1.3.0"
              "zakura-tower-fallback-1.2.0"
            ] (_: zakuraHash);
          };
          nativeBuildInputs = with pkgs; [
            protobuf
            pkg-config
            cmake
            rustPlatform.bindgenHook
          ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          doCheck = false;
          meta.mainProgram = "ztreamerd";
        };
      });

      devShells = eachSystem (pkgs: {
        default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            rustc
            cargo
            rustfmt
            clippy
            git
            protobuf
            pkg-config
            cmake
            rustPlatform.bindgenHook
          ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
      });
    };
}
