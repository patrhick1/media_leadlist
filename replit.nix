{ pkgs }: {
  deps = [
    pkgs.bash                     # Basic shell utilities
    pkgs.libiconv                 # Character set conversion library, often a dependency
    # pkgs.libxcrypt              # Cryptography library, include if issues persist or packages need it
    
    pkgs.python311                # Python 3.11
    pkgs.python311Packages.pip    # Pip for Python 3.11
    # Add other essential Python build tools if needed by specific packages in requirements.txt
    # pkgs.python311Packages.setuptools 
    # pkgs.python311Packages.wheel

    pkgs.nodejs_20                # Node.js v20.x and npm
  ];
  env = {
    PYTHONBIN = "${pkgs.python311}/bin/python3.11"; # Be specific with python3.11
    # Set common env vars that can help with builds
    TERMINFO = "${pkgs.ncurses}/share/terminfo"; # For ncurses-based programs in shell
    CONFIG_SHELL = "${pkgs.bash}/bin/bash";
    PYTHON_LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc.lib pkgs.zlib ]; # Common libs
    MPLCONFIGDIR = "/tmp/matplotlib";
  };
} 