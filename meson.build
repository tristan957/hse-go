project(
    'hse-go',
    version: '1.0.0',
    meson_version: '>=0.57.0',
)

version_components = meson.project_version().split('.')

hse_go_major_version = version_components[0]
hse_go_minor_version = version_components[1]
hse_go_patch_version = version_components[2]

fs = import('fs')

go = find_program('go', required: true)

hse_dep = dependency(
    'hse-1',
    version: '>=1.10.0',
    required: false,
)

depends = []
cgo_env = environment()

if get_option('hse-include-dir') != '' or get_option('hse-lib-dir') != ''
    if get_option('hse-include-dir') != ''
        cgo_env.set('CGO_CFLAGS', '-I@0@'.format(get_option('hse-include-dir')))
    endif
    if get_option('hse-lib-dir') != ''
        cgo_env.set('CGO_LDFLAGS', '-L@0@'.format(get_option('hse-lib-dir')))
    endif
else
    if hse_dep.found()
        cgo_env.set('CGO_CFLAGS', '-I@0@'.format(hse_dep.get_variable(pkgconfig: 'includedir')))
        cgo_env.set('CGO_LDFLAGS', '-L@0@'.format(hse_dep.get_variable(pkgconfig: 'libdir')))
    else
        hse_proj = subproject(
            'hse',
            default_options: [
                'b_ndebug=true',
                'buildtype=release',
                'default_library=shared',
                'cli=false',
                'samples=false',
                'tests=false',
                'tools=false',
                'bindings-python=false',
            ],
        )
        hse = hse_proj.get_variable('hse')
        hse_dep = hse_proj.get_variable('hse_dep')

        depends += hse

        cgo_env.set('CGO_CFLAGS', '-I@0@'.format(hse_dep.get_variable(internal: 'source-root') / 'include'))
        cgo_env.set('CGO_LDFLAGS', '-lhse-1 -L@0@'.format(fs.parent(hse.full_path())))
    endif
endif

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
    output: 'hse-go.ar'.format(hse_go_major_version),
    depends: depends,
    depend_files: files(
        'cursor.go',
        'kvdb.go',
        'kvs.go',
        'params.go',
        'transaction.go',
        'experimental' / 'kvdb.go',
        'experimental' / 'kvs.go',
        'experimental' / 'params.go',
        'limits' / 'limits.go',
    ),
    env: cgo_env,
)

if get_option('tests')
    # Set LD_LIBRARY_PATH so that at runtime of the tests, the hse library is
    # found.
    cgo_env.set('LD_LIBRARY_PATH', hse_dep.get_variable(internal: 'build-root') / 'lib')

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
        workdir: meson.project_source_root(),
        is_parallel: false,
        env: cgo_env,
    )
endif
