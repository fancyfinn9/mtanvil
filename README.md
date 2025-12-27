# mtanvil

<img alt="GitHub License" src="https://img.shields.io/github/license/fancyfinn9/mtanvil?color=darkgreen"> <a href="https://pypi.org/project/mtanvil/" target="_blank"><img alt="PyPI - Version" src="https://img.shields.io/pypi/v/mtanvil?logo=python&logoColor=white&color=blue"></a>

A Python library for parsing and editing Luanti worlds.

The name comes from Luanti’s former name (‘MT’ for Minetest) and the Minecraft world parsing library ‘anvil’

> mtanvil is in alpha, so if you decide to use it then be aware of the following:
>
> Future updates _will_ have breaking changes.
>
> Testing is welcome, please do open an issue if you find any bugs or problems.
>
> Features that are not supported (yet):
> * Some node inventories fail to load, such as the furnace in Minetest Game. I am researching this constantly and will have a solution to this very soon.
> * Older MapBlock formats (<29) may not load and/or serialize correctly (due to the lack of documentation of the specifics of their zlib compression). This will be fixed soon.
> * Older MapBlock formats (<23) will lose node metadata due to the old and new formats not being directly compatible. The conversion will be figured out in the future.

mtanvil fully supports MapBlock format version 29 (latest). Other versions may not be fully supported right now but should receive full support in the future.

It is recommended that you familiarize yourself with the [Map File Format and MapBlock Serialization Format](https://github.com/luanti-org/luanti/blob/master/doc/world_format.md#map-file-format) so that you fully understand what data mtanvil provides.

## Installation

Install mtanvil by running

`pip install mtanvil`

in your terminal.

## Docs

You can find the comprehensive mtanvil docs [here](https://github.com/fancyfinn9/mtanvil/wiki).

Please do open an issue if you find that something has not been documented properly.
