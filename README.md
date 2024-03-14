# yt-dlpp
A thin wrapper around [`yt-dlp`](https://github.com/yt-dlp/yt-dlp) for parallel downloads

## Description

`yt-dlpp` is just `yt-dlp` but starts downloads in parallel.   
It supports passing multiple download URLs and unwrapping playlists.

## Installation

### Build locally on Linux

```sh
git clone "$YT_DLPP_REPO"
cd yt-dlpp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install 
```

## Usage

`yt-dlpp` accepts all valid `yt-dlp` arguments and passes them mostly intact.  
However, arguments related to CLI output will be ignored since that is handled by `yt-dlpp`.

Below, you can find a short list of the added arguments:

| Argument | Description | Default value |
| - | - | - |
| `--n-info-workers` | Number concurrent url info extraction workers | Number of CPUs in the system |
| `--n-dl-workers` | Number concurrent download workers | Number of CPUs in the system |

## Reason to exist

As you may be aware, starting multiple Youtube downloads in parallel will probably not change the overall time taken, since a rate limit is in place, and before reaching it, you may simply reach your ISP's limit.  
But `yt-dlp` (and therefore `yt-dlpp` too) is not only used for Youtube! Other websites have different restrictions, and some may simply allow downloads at a slow speed, but no per IP throttling (eg. NHK World Japan).

