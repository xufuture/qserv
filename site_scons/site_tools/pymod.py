import distutils.sysconfig
import logging
import os
from sets import Set
import SCons.Action
from SCons.Script import Touch

import utils

def install_python_module(env, target, source):
    """ Define targets which will install all python file contained in
        source_dir_path and sub-directories in python_path_prefix.
    """
    log = logging.getLogger()

    python_path_prefix=target
    source_dir_path=source
    env['pythonpath'] =distutils.sysconfig.get_python_lib(prefix=python_path_prefix)
    target_lst = []
    clean_target_lst = []

    source_lst = utils.recursive_glob(source_dir_path,'*.py',env)

    for f in source_lst :
        target = utils.replace_base_path(source_dir_path,env['pythonpath'],f,env)
        env.InstallAs(target, f)
        target_lst.append(target)
        # .pyc files will also be removed
        env.Clean(target, "%s%s" % (target ,"c"))

        #print "AddPostAction to target %s" % target

    add_empty_init_file_if_needed(target_lst,env)
    clean_python_path_dir(target_lst,env)

    return target_lst

def add_empty_init_file_if_needed(target_lst, env):
    analysed_py_modules = []
    str_target_lst = map(str,target_lst)
    for target in str_target_lst:
        module_dir = os.path.dirname(target)
        while module_dir != env['pythonpath']:
            if module_dir not in analysed_py_modules:
                module_file = os.path.join(module_dir,"__init__.py")
                if not os.path.exists(module_file) and module_file not in str_target_lst:
                    env.AddPostAction(target_lst[-1], Touch(module_file))
                analysed_py_modules.append(module_dir)
            module_dir = os.path.dirname(module_dir)

def is_empty_init_file(node):
 return (os.path.basename(node) == "__init__.py" and os.stat(node).st_size==0)

def clean_python_path_dir(target_lst,env):
    """ Delete directories relative to current targets in PYTHONPATH during cleaning
    """
    cleaned_py_dirs = Set()
    bottom_up_target_list = map(str,target_lst)[::-1]
    analysed_py_modules = []

    # Don't remove directory if it contains other files than current targets
    for target in  bottom_up_target_list:
        module_dir = os.path.dirname(target)
        if  os.path.exists(module_dir) and module_dir not in analysed_py_modules:
            other_python_modules = []
            for e in os.listdir(module_dir):
                node = os.path.join(module_dir,e)
                if (node not in bottom_up_target_list
                    and node not in cleaned_py_dirs
                    and not is_empty_init_file(node)
                    ):
                        other_python_modules.append(node)
            if other_python_modules == []:
                cleaned_py_dirs.add(module_dir)
            analysed_py_modules.append(module_dir)

    # TODO : check that this
    # remove empty parent dirs until PYTHON_PATH if needed
    # take common prefix of empty_py_dirs and check if upper dir to python_path are empty
    # Keep __init__.py file if a subdirectory contains at least one file of bottom_up_target_list
    for d in cleaned_py_dirs:
        env.Clean(target_lst[0], d)

    return ""


def generate(env):
    env.AddMethod(install_python_module,'InstallPythonModule')

def exists(env):
    return env.Detect('python')
