# [2.1.0] (2020-01-19)

### Changes

* [Breaking] Store implementations must now be registered with Nebular during startup for ASP.NET Core. E.g., `services.AddNebulaStore<FlowerStore>()`.
* Improved handling of store configuration updates ([#22](https://github.com/cloud-maker-ai/Nebula/issues/22)) ([ca451e6](https://github.com/cloud-maker-ai/Nebula/commit/ca451e6))

# [2.0.0] (2019-11-24)

### Changes

* Added CHANGELOG.md ([#4](https://github.com/cloud-maker-ai/Nebula/issues/4))
* Re-instated latest optimisation and fixed document timestamp handling ([#15](https://github.com/cloud-maker-ai/Nebula/issues/15)) ([7363b75](https://github.com/cloud-maker-ai/Nebula/commit/7363b75))

# [1.0.3] (2019-09-04)

### Changes

* Fixed document timestamp handling and removed latest optimisation ([#16](https://github.com/cloud-maker-ai/Nebula/issues/16)) ([497c28c](https://github.com/cloud-maker-ai/Nebula/commit/497c28c))