{
  description = "Rust devshell";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-26.05";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      rustVersion = "1.98.1";

      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ (import rust-overlay) ];
      };
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = with pkgs; [
          rust-bin.stable.${rustVersion}.complete
          buildah
          cargo-nextest
          just
          podman
          sqlx-cli
        ];
        shellHook = ''
          export ARR_UPGRADE_CONFIG="./config/"
          # for sqlx-cli
          export DATABASE_URL=sqlite://./config/arr_upgrade.db
        '';
      };
    };
}
