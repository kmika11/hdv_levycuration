# H2 # Error 1

`Dataset with pid 'doi:10.7910/DVN/DBJHSC' created.


╭───────────── DVUploader ──────────────╮
│ Server: https://dataverse.harvard.edu │
│ PID: doi:10.7910/DVN/DBJHSC           │
│ Files: 279                            │
╰───────────────────────────────────────╯
   🔎 Checking   
  dataset files  
┏━━━━━┳━━━━━━━━━┓
┃ New ┃ Replace ┃
┡━━━━━╇━━━━━━━━━┩
│ 279 │ 0       │
└─────┴─────────┘
🚀 Uploading files

╰── Registering files ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━   0% -:--:--
---------------------------------------------------------------------------
HTTPStatusError                           Traceback (most recent call last)
Cell In[17], line 7
      5 dataset.license = dataverse.licenses["CC BY-NC-ND 4.0"]
      6 #upload datasets and files
----> 7 pid = dataset.upload(dataverse_name = dv_collection, n_parallel=2)
      8 collection_pids[dataset_title] = pid

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/easyDataverse/dataset.py:252, in Dataset.upload(self, dataverse_name, n_parallel)
    240 """Uploads a given dataset to a Dataverse installation specified in the environment variable.
    241 
    242 Args:
   (...)
    247     str: The identifier of the uploaded dataset.
    248 """
    250 self._validate_required_fields()
--> 252 self.p_id = upload_to_dataverse(
    253     json_data=self.dataverse_json(),
    254     dataverse_name=dataverse_name,
    255     files=self.files,
    256     p_id=self.p_id,
    257     DATAVERSE_URL=str(self.DATAVERSE_URL),
    258     API_TOKEN=str(self.API_TOKEN),
    259     n_parallel=n_parallel,
    260 )
    262 return self.p_id

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/easyDataverse/uploader.py:59, in upload_to_dataverse(json_data, dataverse_name, files, p_id, n_parallel, DATAVERSE_URL, API_TOKEN)
     56 # Get response data
     57 p_id = response.json()["data"]["persistentId"]
---> 59 _uploadFiles(
     60     files=files,
     61     p_id=p_id,
     62     api=api,
     63     n_parallel=n_parallel,
     64 )  # type: ignore
     66 console = Console()
     67 url = urljoin(DATAVERSE_URL, f"dataset.xhtml?persistentId={p_id}")

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/easyDataverse/uploader.py:107, in _uploadFiles(files, p_id, api, n_parallel)
    104     return
    106 dvuploader = DVUploader(files=files)
--> 107 dvuploader.upload(
    108     persistent_id=p_id,
    109     dataverse_url=api.base_url,
    110     api_token=api.api_token,
    111     n_parallel_uploads=n_parallel,
    112 )

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/dvuploader.py:153, in DVUploader.upload(self, persistent_id, dataverse_url, api_token, n_parallel_uploads, force_native, replace_existing, proxy)
    151 else:
    152     with progress:
--> 153         asyncio.run(
    154             direct_upload(
    155                 files=files,
    156                 dataverse_url=dataverse_url,
    157                 api_token=api_token,
    158                 persistent_id=persistent_id,
    159                 pbars=pbars,
    160                 progress=progress,
    161                 n_parallel_uploads=n_parallel_uploads,
    162             )
    163         )
    165 if self.verbose:
    166     rich.print("\n[bold italic white]✅ Upload complete\n")

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/nest_asyncio.py:30, in _patch_asyncio.<locals>.run(main, debug)
     28 task = asyncio.ensure_future(main)
     29 try:
---> 30     return loop.run_until_complete(task)
     31 finally:
     32     if not task.done():

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/nest_asyncio.py:98, in _patch_loop.<locals>.run_until_complete(self, future)
     95 if not f.done():
     96     raise RuntimeError(
     97         'Event loop stopped before Future completed.')
---> 98 return f.result()

File ~/anaconda3/envs/curation/lib/python3.12/asyncio/futures.py:203, in Future.result(self)
    201 self.__log_traceback = False
    202 if self._exception is not None:
--> 203     raise self._exception.with_traceback(self._exception_tb)
    204 return self._result

File ~/anaconda3/envs/curation/lib/python3.12/asyncio/tasks.py:314, in Task.__step_run_and_handle_result(***failed resolving arguments***)
    310 try:
    311     if exc is None:
    312         # We use the `send` method directly, because coroutines
    313         # don't have `__iter__` and `__next__` methods.
--> 314         result = coro.send(None)
    315     else:
    316         result = coro.throw(exc)

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/directupload.py:103, in direct_upload(files, dataverse_url, api_token, persistent_id, progress, pbars, n_parallel_uploads, proxy)
     96 session_params = {
     97     "timeout": None,
     98     "limits": httpx.Limits(max_connections=n_parallel_uploads),
     99     "headers": headers,
    100 }
    102 async with httpx.AsyncClient(**session_params) as session:
--> 103     await _add_files_to_ds(
    104         session=session,
    105         files=files,
    106         dataverse_url=dataverse_url,
    107         pid=persistent_id,
    108         progress=progress,
    109         pbar=pbar,
    110     )

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/directupload.py:560, in _add_files_to_ds(session, dataverse_url, pid, files, progress, pbar)
    556 replace_json_data = _prepare_registration(files, use_replace=True)
    558 if novel_json_data:
    559     # Register new files, if any
--> 560     await _multipart_json_data_request(
    561         session=session,
    562         json_data=novel_json_data,
    563         url=novel_url,
    564     )
    566 if replace_json_data:
    567     # Register replacement files, if any
    568     await _multipart_json_data_request(
    569         session=session,
    570         json_data=replace_json_data,
    571         url=replace_url,
    572     )

File ~/anaconda3/envs/curation/lib/python3.12/site-packages/dvuploader/directupload.py:630, in _multipart_json_data_request(json_data, url, session)
    627 response = await session.post(url, files=files)
    629 if not response.is_success:
--> 630     raise httpx.HTTPStatusError(
    631         f"Failed to register files: {response.text}",
    632         request=response.request,
    633         response=response,
    634     )

HTTPStatusError: Failed to register files: {"status":"ERROR","message":"CommandException updating DatasetVersion from addFiles job: Command edu.harvard.iq.dataverse.engine.command.impl.UpdateDatasetVersionCommand@cb8ac96 failed: One or more Bean Validation constraints were violated while executing Automatic Bean Validation on callback event: prePersist for class: edu.harvard.iq.dataverse.FileMetadata. Please refer to the embedded constraint violations for details."}`

