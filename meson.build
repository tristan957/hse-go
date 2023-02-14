# SPDX-License-Identifier: Apache-2.0 OR MIT
#
# SPDX-FileCopyrightText: Copyright 2022 Micron Technology, Inc.

project(
    'hse-go',
    version: '3.1.0',
    meson_version: '>=0.58.0',
    default_options: [
        'prefix=/opt/hse',
        'buildtype=debugoptimized',
        'warning_level=2',
        'force_fallback_for=lz4,cjson',
    ]
)

version_components = meson.project_version().split('.')

hse_go_major_version = version_components[0].to_int()
hse_go_minor_version = version_components[1].to_int()
hse_go_patch_version = version_components[2].to_int()

fs = import('fs')

go = find_program('go', required: true)

hse_dep = dependency(
    'hse-@0@'.format(hse_go_major_version),
    version: [
        '>=@0@.0.0'.format(hse_go_major_version),
        '<@0@.0.0'.format(hse_go_major_version + 1)
    ],
    required: false
)

depends = []
cgo_env = environment()

if not hse_dep.found()
    hse_proj = subproject(
        'hse',
        default_options: [
            'default_library=static',
            'warning_level=0',
            'werror=false',
            'cli=false',
            'docs=disabled',
            'tests=false',
            'tools=disabled',
            'samples=false',
            'bindings=none',
        ]
    )
    hse_dep = hse_proj.get_variable('hse_dep')

    depends += hse_proj.get_variable('hse')
    libdir = meson.global_build_root() / 'subprojects/hse/lib'

    cgo_env.set('CGO_CFLAGS', '-I@0@ -I@1@'.format(meson.global_source_root() / 'subprojects/hse/include', meson.global_build_root() / 'subprojects/hse/include'))
else
    cgo_env.set('CGO_CFLAGS', '-I' + hse_dep.get_variable('includedir'))
endif

cgo_env.set('CGO_LDFLAGS', '-l hse-@0@ -L@1@'.format(hse_go_major_version, libdir))

hse_go = custom_target(
    'hse-go',
    build_by_default: true,
    command: [
        go,
        'build',
        '-o',
        '@OUTPUT@',
        '@SOURCE_ROOT@',
    ],
    output: 'hse-go.ar',
    depends: depends,
    depend_files: files(
        'cursor.go',
        'hse.go',
        'kvdb.go',
        'kvs.go',
        'transaction.go',
        'experimental' / 'kvdb.go',
        'experimental' / 'kvs.go',
        'limits' / 'limits.go'
    ),
    env: cgo_env
)

if get_option('tests')
    # Set LD_LIBRARY_PATH so that at runtime of the tests, the hse library is
    # found.
    cgo_env.set('LD_LIBRARY_PATH', libdir)

    # Go testing is stupid, and if I don't run the tests with ./..., I need to
    # import github.com/hse-project/hse-go, and then I end up with an import
    # cycle. Thanks Google!
    #
    # In a perfect world, we would split up the tests based on X_test.go. I
    # don't think this is super important however due to the fact that this
    # Meson build system for hse-go is supplemental in order to support the
    # hse repo and have hse-go sit as a subproject.
    #
    # I think there could be a way around this issue by naming all tests as
    # TestX..., and then doing a test run using regex to pick out the tests for
    # each suite.
    test(
        'bindings',
        go,
        args: [
            'test',
            './...',
        ],
        depends: [
            hse_go,
        ],
        suite: ['unit'],
        workdir: meson.project_source_root(),
        is_parallel: false,
        env: cgo_env
    )
endif
