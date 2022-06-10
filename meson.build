project(
    'hse-go',
    version: '2.2.0',
    meson_version: '>=0.58.0',
    default_options: [
        'prefix=/opt/hse',
        'c_std=gnu11', # necessary until Meson 0.63.0
        'buildtype=debugoptimized',
        'warning_level=2',
        'force_fallback_for=xxhash,lz4,cjson',
    ]
)

version_components = meson.project_version().split('.')

hse_go_major_version = version_components[0]
hse_go_minor_version = version_components[1]
hse_go_patch_version = version_components[2]

fs = import('fs')

go = find_program('go', required: true)

hse_dep = dependency(
    'hse-2',
    version: '>=2.2.0',
    required: false
)

depends = []
cgo_env = environment()

if not hse_dep.found()
    hse_proj = subproject(
        'hse',
        default_options: [
            'cli=false',
            'docs=disabled',
            'tests=false',
            'tools=disabled',
            'samples=false',
            'bindings=none',
            'db_bench=false',
        ]
    )
    hse_dep = hse_proj.get_variable('hse_dep')

    depends += hse_proj.get_variable('hse')

    cgo_env.set('CGO_CFLAGS', '-I@0@ -I@1@'.format(hse_dep.get_variable('includedir'), meson.global_build_root() / 'subprojects/hse/include'))
else
    cgo_env.set('CGO_CFLAGS', '-I' + hse_dep.get_variable('includedir'))
endif

cgo_env.set('CGO_LDFLAGS', '-l hse-@0@ -L@1@'.format(hse_go_major_version, hse_dep.get_variable('libdir')))

hse_go = custom_target(
    'build',
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
        'limits' / 'limits.go',
    ),
    env: cgo_env
)

if get_option('tests')
    # Set LD_LIBRARY_PATH so that at runtime of the tests, the hse library is
    # found.
    cgo_env.set('LD_LIBRARY_PATH', hse_dep.get_variable('libdir'))

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
        env: cgo_env,
    )
endif
