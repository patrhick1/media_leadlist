{ pkgs }: {
  deps = [
    pkgs.python311Packages.pip  # Installs pip for Python 3.11
    pkgs.python311              # Installs Python 3.11 itself
    pkgs.nodejs_20              # Installs Node.js version 20.x.x and npm
    # You can specify other versions like pkgs.python310Packages.pip, pkgs.nodejs_18, etc.
  ];
  env = {
    PYTHONBIN = "${pkgs.python311}/bin/python3"; # Sets an environment variable for Python binary
    PYTHON_SITE_PACKAGES = ".replit/pypoetry/lib/python3.11/site-packages"; # Common for Replit Python
    MPLCONFIGDIR = "/tmp/matplotlib"; # Often needed if any package uses matplotlib
  };
} 