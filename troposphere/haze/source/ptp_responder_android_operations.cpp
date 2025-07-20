/*
 * Copyright (c) Atmosphère-NX
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <haze.hpp>
#include <haze/ptp_data_builder.hpp>
#include <haze/ptp_data_parser.hpp>
#include <haze/ptp_responder_types.hpp>
#include <haze/threaded_file_transfer.hpp>

namespace haze {

    Result PtpResponder::GetPartialObject64(PtpDataParser &dp) {
        PtpDataBuilder db(m_buffers->usb_bulk_write_buffer, std::addressof(m_usb_server));

        /* Get the object ID, offset, and size for the file we want to read. */
        u32 object_id, size;
        u64 offset;
        R_TRY(dp.Read(std::addressof(object_id)));
        R_TRY(dp.Read(std::addressof(offset)));
        R_TRY(dp.Read(std::addressof(size)));
        R_TRY(dp.Finalize());

        /* Check if we know about the object. If we don't, it's an error. */
        auto * const obj = m_object_database.GetObjectById(object_id);
        R_UNLESS(obj != nullptr, haze::ResultInvalidObjectId());

        /* Lock the object as a file. */
        FsFile file;
        R_TRY(m_fs.OpenFile(obj->GetName(), FsOpenMode_Read, std::addressof(file)));

        /* Ensure we maintain a clean state on exit. */
        ON_SCOPE_EXIT { m_fs.CloseFile(std::addressof(file)); };

        /* Get the file's size. */
        s64 file_size = 0;
        R_TRY(m_fs.GetFileSize(std::addressof(file), std::addressof(file_size)));

        /* Ensure the requested offset and size are within range. */
        R_UNLESS(offset + size > offset, haze::ResultInvalidArgument());
        R_UNLESS(static_cast<u64>(file_size) <= offset + size, haze::ResultInvalidArgument());

        /* Send the header and data size. */
        R_TRY(db.AddDataHeader(m_request_header, size));

        /* Begin reading the file, writing data to the builder as we progress. */
        R_TRY(sphaira::thread::Transfer(offset, offset + size,
            [this, &file, &obj](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                /* Get the next batch. */
                R_RETURN(m_fs.ReadFile(std::addressof(file), off, data, size, FsReadOption_None, bytes_read));
            },
            [this, &db](const void* data, s64 off, s64 size) -> Result {
                /* Write to output. */
                R_RETURN(db.AddBuffer((const u8*)data, size));
            }, sphaira::thread::Mode::SingleThreadedIfSmaller
        ));

        /* Flush the data response. */
        R_TRY(db.Commit());

        /* Write the success response. */
        R_RETURN(this->WriteResponse(PtpResponseCode_Ok));
    }

    Result PtpResponder::SendPartialObject(PtpDataParser &rdp) {
        /* Get the object ID, offset, and size for the file we want to write. */
        u32 object_id, size;
        u64 offset;
        R_TRY(rdp.Read(std::addressof(object_id)));
        R_TRY(rdp.Read(std::addressof(size)));
        R_TRY(rdp.Read(std::addressof(offset)));
        R_TRY(rdp.Finalize());

        /* Check if we know about the object. If we don't, it's an error. */
        auto * const obj = m_object_database.GetObjectById(m_send_object_id);
        R_UNLESS(obj != nullptr, haze::ResultInvalidObjectId());

        /* Lock the object as a file. */
        FsFile file;
        R_TRY(m_fs.OpenFile(obj->GetName(), FsOpenMode_Write | FsOpenMode_Append, std::addressof(file)));

        /* Ensure we maintain a clean state on exit. */
        ON_SCOPE_EXIT { m_fs.CloseFile(std::addressof(file)); };

        /* Get the file's size. */
        s64 file_size = 0;
        R_TRY(m_fs.GetFileSize(std::addressof(file), std::addressof(file_size)));

        /* Ensure the requested offset and size are within range. */
        R_UNLESS(offset + size > offset, haze::ResultInvalidArgument());
        R_UNLESS(static_cast<u64>(file_size) <= offset, haze::ResultInvalidArgument());

        /* Prepare a data parser for the data we are about to receive. */
        PtpDataParser dp(m_buffers->usb_bulk_read_buffer, std::addressof(m_usb_server));

        /* Ensure we have a data header. */
        PtpUsbBulkContainer data_header;
        R_TRY(dp.Read(std::addressof(data_header)));
        R_UNLESS(data_header.type == PtpUsbBulkContainerType_Data,  haze::ResultUnknownRequestType());
        R_UNLESS(data_header.code == m_request_header.code,         haze::ResultOperationNotSupported());
        R_UNLESS(data_header.trans_id == m_request_header.trans_id, haze::ResultOperationNotSupported());

        /* Begin writing to the filesystem. */
        bool is_done = false;
        R_TRY(sphaira::thread::Transfer(offset, offset + size,
            [this, &dp, &is_done](void* data, s64 off, s64 size, u64* bytes_read) -> Result {
                if (is_done) {
                    *bytes_read = 0;
                    R_SUCCEED();
                }

                /* Read as many bytes as we can. */
                u32 bytes_received;
                const Result read_res = dp.ReadBuffer((u8*)data, size, std::addressof(bytes_received));
                *bytes_read = bytes_received;

                /* If we received fewer bytes than the batch size, we're done. */
                if (haze::ResultEndOfTransmission::Includes(read_res)) {
                    is_done = true;
                    R_SUCCEED();
                }

                R_RETURN(read_res);
            },
            [this, &file, &obj, &offset](const void* data, s64 off, s64 size) -> Result {
                /* Write to the file. */
                R_TRY(m_fs.WriteFile(std::addressof(file), offset, data, size, 0));
                offset += size;
                R_SUCCEED();
            }, sphaira::thread::Mode::SingleThreadedIfSmaller
        ));

        /* Write the success response. */
        R_RETURN(this->WriteResponse(PtpResponseCode_Ok));
    }

    Result PtpResponder::TruncateObject(PtpDataParser &dp) {
        /* Get the object ID and size for the file we want to truncate. */
        u32 object_id;
        u64 size;
        R_TRY(dp.Read(std::addressof(object_id)));
        R_TRY(dp.Read(std::addressof(size)));
        R_TRY(dp.Finalize());

        /* Check if we know about the object. If we don't, it's an error. */
        auto * const obj = m_object_database.GetObjectById(object_id);
        R_UNLESS(obj != nullptr, haze::ResultInvalidObjectId());

        /* Lock the object as a file. */
        FsFile file;
        R_TRY(m_fs.OpenFile(obj->GetName(), FsOpenMode_Write, std::addressof(file)));

        /* Ensure we maintain a clean state on exit. */
        ON_SCOPE_EXIT { m_fs.CloseFile(std::addressof(file)); };

        /* Truncate the file. */
        R_TRY(m_fs.SetFileSize(std::addressof(file), size));

        /* Write the success response. */
        R_RETURN(this->WriteResponse(PtpResponseCode_Ok));
    }

    Result PtpResponder::BeginEditObject(PtpDataParser &dp) {
        /* Get the object ID we are going to begin editing. */
        u32 object_id;
        R_TRY(dp.Read(std::addressof(object_id)));
        R_TRY(dp.Finalize());

        /* Check if we know about the object. If we don't, it's an error. */
        auto * const obj = m_object_database.GetObjectById(object_id);
        R_UNLESS(obj != nullptr, haze::ResultInvalidObjectId());

        /* We don't implement transactions, so write the success response. */
        R_RETURN(this->WriteResponse(PtpResponseCode_Ok));
    }

    Result PtpResponder::EndEditObject(PtpDataParser &dp) {
        /* Get the object ID we are going to finish editing. */
        u32 object_id;
        R_TRY(dp.Read(std::addressof(object_id)));
        R_TRY(dp.Finalize());

        /* Check if we know about the object. If we don't, it's an error. */
        auto * const obj = m_object_database.GetObjectById(object_id);
        R_UNLESS(obj != nullptr, haze::ResultInvalidObjectId());

        /* We don't implement transactions, so write the success response. */
        R_RETURN(this->WriteResponse(PtpResponseCode_Ok));
    }


}
