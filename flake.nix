{
  description = "sisyphus pushes your changes to production";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };
  outputs = {
    self,
    nixpkgs,
    ...
  }: let
    systems = nixpkgs.lib.platforms.unix;
    forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system);
  in {
    packages = forAllSystems (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        bazelisk = pkgs.bazelisk;
        google-cloud-sdk =
          pkgs.google-cloud-sdk.withExtraComponents [
            pkgs.google-cloud-sdk.components.gke-gcloud-auth-plugin
          ];
        kubectl = pkgs.kubectl;
        python3 = pkgs.python3;
        rustup = pkgs.rustup;
        sqlite = pkgs.sqlite;
      });
    devShells = forAllSystems (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        packages = builtins.attrValues self.packages.${system};
      in {
        default = pkgs.mkShell {
          packages = packages;
          shellHook = ''
            alias bazel=bazelisk
            export PS1="\[\033[1;32m\][s:\w]\$\[\033[0m\] "
          '';
        };
      });
  };
}
